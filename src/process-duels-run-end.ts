import { AllCardsService, GameFormat, normalizeDuelsHeroCardId } from '@firestone-hs/reference-data';
import { DeckDefinition, decode, encode } from 'deckstrings';
import { ServerlessMysql } from 'serverless-mysql';
import SqlString from 'sqlstring';
import { getConnection } from './db/rds';
import { ReviewMessage } from './review-message';

const cards = new AllCardsService();

// This example demonstrates a NodeJS 8.10 async handler[1], however of course you could use
// the more traditional callback-style handler.
// [1]: https://aws.amazon.com/blogs/compute/node-js-8-10-runtime-now-available-in-aws-lambda/
export default async (event): Promise<any> => {
	const messages: readonly ReviewMessage[] = (event.Records as any[])
		.map(event => JSON.parse(event.body))
		.reduce((a, b) => a.concat(b), [])
		.filter(event => event)
		.map(event => event.Message)
		.filter(msg => msg)
		.map(msg => JSON.parse(msg));
	const mysql = await getConnection();
	for (const message of messages) {
		await handleReview(message, mysql);
	}
	await mysql.end();
	return { statusCode: 200, body: null };
};

const handleReview = async (message: ReviewMessage, mysql: ServerlessMysql): Promise<void> => {
	if (message.gameMode !== 'paid-duels') {
		console.log('not heroic duels', message);
		return;
	}

	const runId = message.currentDuelsRunId ?? message.runId;
	if (!runId) {
		console.error('runId empty', message);
		return;
	}

	const lootQuery = `
		SELECT bundleType, 
		CASE  
			WHEN chosenOptionIndex = 1 THEN option1 
			WHEN chosenOptionIndex = 2 THEN option2  
			ELSE option3 END as pickedTreasure 
		FROM dungeon_run_loot_info
		WHERE runId = '${runId}'
		AND bundleType IN ('treasure', 'hero-power', 'signature-treasure') 
	`;
	const lootResults: readonly any[] = await mysql.query(lootQuery);
	if (!lootResults?.length) {
		return;
	}

	const query = `
		SELECT x1.creationDate, x1.playerClass, x1.playerCardId, x1.playerRank, x1.playerDecklist, x1.additionalResult
		FROM replay_summary x1 
		WHERE x1.runId = '${runId}'
		AND x1.playerDecklist IS NOT null 
	`;
	const allDecksResults: readonly any[] = await mysql.query(query);
	const decksResults = allDecksResults.filter(result => result.additionalResult === '0-0');
	if (!decksResults || decksResults.length !== 1) {
		return;
	}

	// Discard the info if multiple classes are in the same run
	const uniqueHeroes = [...new Set(allDecksResults.map(result => result.playerCardId))];
	if (uniqueHeroes.length !== 1) {
		console.error('corrupted run', runId, uniqueHeroes);
		return;
	}

	const heroPowerNodes = lootResults.filter(result => result.bundleType === 'hero-power');
	if (heroPowerNodes.length !== 1) {
		return;
	}

	const heroPowerNode = heroPowerNodes[0];
	const finalDecklist = message.playerDecklist;
	const [wins, losses] = message.additionalResult.split('-').map(info => parseInt(info));

	const firstGameInRun = decksResults[0];
	const periodDate = new Date(message.creationDate);
	await cards.initializeCardsDb();
	const decklist = cleanDecklist(firstGameInRun.playerDecklist, firstGameInRun.playerCardId, cards);
	if (!decklist) {
		return null;
	}

	const allTreasures = findTreasuresCardIds(lootResults, heroPowerNode.runId);
	const row: InternalDuelsRow = {
		gameMode: message.gameMode,
		runStartDate: new Date(firstGameInRun.creationDate),
		runEndDate: periodDate,
		buildNumber: message.buildNumber,
		rating: firstGameInRun.playerRank,
		runId: runId,
		playerClass: firstGameInRun.playerClass,
		hero: message.playerCardId,
		heroPower: heroPowerNode.pickedTreasure,
		signatureTreasure: findSignatureTreasureCardId(lootResults, heroPowerNode.runId),
		wins: wins + (message.result === 'won' ? 1 : 0),
		losses: losses + (message.result === 'lost' ? 1 : 0),
		treasures: allTreasures
			.filter(cardId => !cards.getCard(cardId)?.mechanics?.includes('DUNGEON_PASSIVE_BUFF'))
			.join(','),
		passives: allTreasures
			.filter(cardId => cards.getCard(cardId)?.mechanics?.includes('DUNGEON_PASSIVE_BUFF'))
			.join(','),
	} as InternalDuelsRow;

	const insertQuery = `
		INSERT INTO duels_stats_by_run 
		(
			gameMode, 
			runStartDate, 
			runEndDate, 
			buildNumber, 
			rating,
			runId,
			playerClass, 
			decklist,
			finalDecklist,
			hero,
			heroPower,
			signatureTreasure,
			treasures,
			passives,
			wins,
			losses
		)
		VALUES 
		(
			${SqlString.escape(row.gameMode)},
			${SqlString.escape(row.runStartDate)}, 
			${SqlString.escape(row.runEndDate)}, 
			${SqlString.escape(row.buildNumber)},
			${SqlString.escape(row.rating)},
			${SqlString.escape(row.runId)},
			${SqlString.escape(row.playerClass)},
			${SqlString.escape(decklist)},
			${SqlString.escape(finalDecklist)},
			${SqlString.escape(row.hero)},
			${SqlString.escape(row.heroPower)},
			${SqlString.escape(row.signatureTreasure)},
			${SqlString.escape(row.treasures)},
			${SqlString.escape(row.passives)},
			${SqlString.escape(row.wins)},
			${SqlString.escape(row.losses)}
		)
	`;
	console.log('running query', insertQuery);
	await mysql.query(insertQuery);
};

const cleanDecklist = (initialDecklist: string, playerCardId: string, cards: AllCardsService): string => {
	const decoded = decode(initialDecklist);
	const validCards = decoded.cards.filter(dbfCardId => cards.getCardFromDbfId(dbfCardId[0]).collectible);
	if (validCards.length !== 15) {
		console.error('Invalid deck list', initialDecklist, decoded);
		return null;
	}
	const hero = getHero(playerCardId, cards);
	const newDeck: DeckDefinition = {
		cards: validCards,
		heroes: !hero ? decoded.heroes : [hero],
		format: GameFormat.FT_WILD,
	};
	const newDeckstring = encode(newDeck);
	return newDeckstring;
};

interface InternalDuelsRow {
	readonly gameMode: 'paid-duels';
	readonly runStartDate: Date;
	readonly runEndDate: Date;
	readonly buildNumber: number;
	readonly rating: number;
	readonly runId: string;
	readonly playerClass: string;
	readonly hero: string;
	readonly heroPower: string;
	readonly signatureTreasure: string;
	readonly wins: number;
	readonly losses: number;
	readonly treasures: string;
	readonly passives: string;
}

const findSignatureTreasureCardId = (decksResults: readonly any[], runId: string): string => {
	const sigs = decksResults
		.filter(result => result.runId === runId)
		.filter(result => result.bundleType === 'signature-treasure');
	return sigs.length === 0 ? null : sigs[0].pickedTreasure;
};

const findTreasuresCardIds = (decksResults: readonly any[], runId: string): readonly string[] => {
	return decksResults
		.filter(result => result.runId === runId)
		.filter(result => result.bundleType === 'treasure')
		.map(result => result.pickedTreasure);
};

const getHero = (playerCardId: string, cards: AllCardsService): number => {
	const normalizedCardId = normalizeDuelsHeroCardId(playerCardId);
	const normalizedCard = cards.getCard(normalizedCardId);
	return normalizedCard?.dbfId ?? 7;
};
