import type {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
} from 'n8n-workflow';
import { NodeApiError, NodeConnectionType, NodeOperationError } from 'n8n-workflow';
import { Database, aql } from 'arangojs';

export class ArangoDB implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'ArangoDB',
		name: 'arangoDb',
		icon: 'file:arangodb.svg',
		group: ['database'],
		version: 1,
		description: 'Perform operations on ArangoDB including documents, vector search, and graphs',
		defaults: {
			name: 'ArangoDB',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'arangoDBCredentialsApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Document',
						value: 'document',
					},
					{
						name: 'Vector Search',
						value: 'vectorSearch',
					},
					{
						name: 'Graph',
						value: 'graph',
					},
					{
						name: 'Custom',
						value: 'custom',
					},
				],
				default: 'document',
			},
			{
				displayName: 'Database',
				name: 'database',
				type: 'string',
				default: '_system',
				required: true,
				description: 'The database to connect to',
			},

			// Document Operations
			{
				displayName: 'Operation',
				name: 'documentOperation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['document'],
					},
				},
				options: [
					{
						name: 'Create',
						value: 'create',
						description: 'Create a new document',
					},
					{
						name: 'Delete',
						value: 'delete',
						description: 'Delete a document',
					},
					{
						name: 'Get',
						value: 'get',
						description: 'Get a document by key',
					},
					{
						name: 'Get Many',
						value: 'getMany',
						description: 'Get multiple documents',
					},
					{
						name: 'Query',
						value: 'query',
						description: 'Execute an AQL query',
					},
					{
						name: 'Replace',
						value: 'replace',
						description: 'Replace a document',
					},
					{
						name: 'Update',
						value: 'update',
						description: 'Update a document',
					}
				],
				default: 'create',
				noDataExpression: true,
			},
			{
				displayName: 'Collection',
				name: 'collection',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['create', 'delete', 'get', 'getMany', 'update', 'replace'],
					},
				},
				default: '',
				required: true,
				description: 'The collection to operate on',
			},
			{
				displayName: 'Document Key',
				name: 'documentKey',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['delete', 'get', 'update', 'replace'],
					},
				},
				default: '',
				required: true,
				description: 'The key of the document',
			},
			{
				displayName: 'Document Data',
				name: 'documentData',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['create', 'update', 'replace'],
					},
				},
				default: '{}',
				required: true,
				description: 'The document data as JSON',
			},
			{
				displayName: 'Return New Document',
				name: 'returnNew',
				type: 'boolean',
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['create', 'update', 'replace'],
					},
				},
				default: true,
				description: 'Whether to return the new document after the operation',
			},
			{
				displayName: 'AQL Query',
				name: 'aqlQuery',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['query'],
					},
				},
				default: '',
				required: true,
				description: 'The AQL query to execute',
				typeOptions: {
					rows: 5,
				},
			},
			{
				displayName: 'Query Parameters',
				name: 'queryParameters',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['query'],
					},
				},
				default: '{}',
				description: 'Parameters to bind to the query',
			},
			{
				displayName: 'Limit',
				name: 'limit',
				type: 'number',
				typeOptions: {
					minValue: 1,
				},
				displayOptions: {
					show: {
						resource: ['document'],
						documentOperation: ['getMany'],
					},
				},
				default: 50,
				description: 'Max number of results to return',
			},

			// Vector Search Operations
			{
				displayName: 'Vector Operation',
				name: 'vectorOperation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
					},
				},
				options: [
					{
						name: 'Search',
						value: 'search',
						description: 'Perform a vector similarity search',
					},
					{
						name: 'Create Index',
						value: 'createIndex',
						description: 'Create a vector search index',
					},
					{
						name: 'Delete Index',
						value: 'deleteIndex',
						description: 'Delete a vector search index',
					},
				],
				default: 'search',
				noDataExpression: true,
			},
			{
				displayName: 'Vector Collection',
				name: 'vectorCollection',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
					},
				},
				default: '',
				required: true,
				description: 'The collection containing vectors',
			},
			{
				displayName: 'Vector Field',
				name: 'vectorField',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
						vectorOperation: ['search', 'createIndex'],
					},
				},
				default: 'vector',
				required: true,
				description: 'The field containing the vector data',
			},
			{
				displayName: 'Query Vector',
				name: 'queryVector',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
						vectorOperation: ['search'],
					},
				},
				default: '[]',
				required: true,
				description: 'The vector to search for (as JSON array)',
			},
			{
				displayName: 'Top K',
				name: 'topK',
				type: 'number',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
						vectorOperation: ['search'],
					},
				},
				default: 10,
				description: 'Number of nearest neighbors to return',
			},
			{
				displayName: 'Index Name',
				name: 'indexName',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
						vectorOperation: ['createIndex', 'deleteIndex'],
					},
				},
				default: '',
				required: true,
				description: 'The name of the vector index',
			},
			{
				displayName: 'Dimensions',
				name: 'dimensions',
				type: 'number',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
						vectorOperation: ['createIndex'],
					},
				},
				default: 128,
				required: true,
				description: 'The number of dimensions in the vectors',
			},
			{
				displayName: 'Distance Metric',
				name: 'distanceMetric',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['vectorSearch'],
						vectorOperation: ['createIndex'],
					},
				},
				options: [
					{
						name: 'Euclidean',
						value: 'euclidean',
					},
					{
						name: 'Cosine',
						value: 'cosine',
					},
					{
						name: 'Manhattan',
						value: 'manhattan',
					},
				],
				default: 'euclidean',
				description: 'The distance metric to use',
			},

			// Graph Operations
			{
				displayName: 'Graph Operation',
				name: 'graphOperation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['graph'],
					},
				},
				options: [
					{
						name: 'Add Edge',
						value: 'addEdge',
						description: 'Add an edge to the graph',
					},
					{
						name: 'Add Vertex',
						value: 'addVertex',
						description: 'Add a vertex to the graph',
					},
					{
						name: 'Create Graph',
						value: 'createGraph',
						description: 'Create a new graph',
					},
					{
						name: 'Delete Graph',
						value: 'deleteGraph',
						description: 'Delete a graph',
					},
					{
						name: 'Get Neighbors',
						value: 'getNeighbors',
						description: 'Get neighbors of a vertex',
					},
					{
						name: 'Shortest Path',
						value: 'shortestPath',
						description: 'Find shortest path between vertices',
					},
					{
						name: 'Traverse',
						value: 'traverse',
						description: 'Traverse the graph',
					}
				],
				default: 'traverse',
				noDataExpression: true,
			},

			// Custom Operations
			{
				displayName: 'Custom Operation',
				name: 'customOperation',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['custom'],
					},
				},
				options: [
					{
						name: 'Execute AQL',
						value: 'executeAql',
						description: 'Execute a custom AQL query',
					},
					{
						name: 'Run Transaction',
						value: 'runTransaction',
						description: 'Run a custom transaction',
					},
					{
						name: 'Bulk Operation',
						value: 'bulkOperation',
						description: 'Execute bulk operations',
					},
				],
				default: 'executeAql',
				noDataExpression: true,
			},
			{
				displayName: 'AQL Query',
				name: 'customAqlQuery',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['executeAql'],
					},
				},
				default: '',
				required: true,
				description: 'The custom AQL query to execute',
				typeOptions: {
					rows: 10,
				},
			},
			{
				displayName: 'Bind Variables',
				name: 'bindVariables',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['executeAql', 'runTransaction'],
					},
				},
				default: '{}',
				description: 'Variables to bind to the query or transaction',
			},
			{
				displayName: 'Transaction Collections',
				name: 'transactionCollections',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['runTransaction'],
					},
				},
				default: '{"read": [], "write": [], "exclusive": []}',
				required: true,
				description: 'Collections involved in the transaction',
			},
			{
				displayName: 'Transaction Action',
				name: 'transactionAction',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['runTransaction'],
					},
				},
				default: '',
				required: true,
				description: 'The transaction action code (JavaScript function as string)',
				typeOptions: {
					rows: 10,
				},
			},
			{
				displayName: 'Bulk Operations',
				name: 'bulkOperations',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['bulkOperation'],
					},
				},
				default: '[{"operation": "insert", "collection": "myCollection", "data": []}]',
				required: true,
				description: 'Array of bulk operations to execute',
				typeOptions: {
					rows: 10,
				},
			},
			{
				displayName: 'Return Count',
				name: 'returnCount',
				type: 'boolean',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['executeAql'],
					},
				},
				default: false,
				description: 'Whether to return the count of results',
			},
			{
				displayName: 'Batch Size',
				name: 'batchSize',
				type: 'number',
				displayOptions: {
					show: {
						resource: ['custom'],
						customOperation: ['executeAql'],
					},
				},
				default: 1000,
				description: 'Number of results to return in each batch',
			},
			
			{
				displayName: 'Graph Name',
				name: 'graphName',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['graph'],
					},
				},
				default: '',
				required: true,
				description: 'The name of the graph',
			},
			{
				displayName: 'Vertex Collections',
				name: 'vertexCollections',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['createGraph'],
					},
				},
				default: '',
				required: true,
				description: 'Comma-separated list of vertex collections',
			},
			{
				displayName: 'Edge Definitions',
				name: 'edgeDefinitions',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['createGraph'],
					},
				},
				default: '[{"collection": "edges", "from": ["vertices"], "to": ["vertices"]}]',
				required: true,
				description: 'Edge definitions as JSON array',
			},
			{
				displayName: 'Vertex Data',
				name: 'vertexData',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['addVertex'],
					},
				},
				default: '{}',
				required: true,
				description: 'The vertex data as JSON',
			},
			{
				displayName: 'Vertex Collection',
				name: 'vertexCollection',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['addVertex'],
					},
				},
				default: '',
				required: true,
				description: 'The vertex collection to add to',
			},
			{
				displayName: 'Edge Data',
				name: 'edgeData',
				type: 'json',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['addEdge'],
					},
				},
				default: '{"_from": "collection/key", "_to": "collection/key"}',
				required: true,
				description: 'The edge data as JSON (must include _from and _to)',
			},
			{
				displayName: 'Edge Collection',
				name: 'edgeCollection',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['addEdge'],
					},
				},
				default: '',
				required: true,
				description: 'The edge collection to add to',
			},
			{
				displayName: 'Start Vertex',
				name: 'startVertex',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['traverse', 'shortestPath', 'getNeighbors'],
					},
				},
				default: '',
				required: true,
				description: 'The starting vertex (format: collection/key)',
			},
			{
				displayName: 'End Vertex',
				name: 'endVertex',
				type: 'string',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['shortestPath'],
					},
				},
				default: '',
				required: true,
				description: 'The ending vertex (format: collection/key)',
			},
			{
				displayName: 'Direction',
				name: 'direction',
				type: 'options',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['traverse', 'getNeighbors'],
					},
				},
				options: [
					{
						name: 'Outbound',
						value: 'outbound',
					},
					{
						name: 'Inbound',
						value: 'inbound',
					},
					{
						name: 'Any',
						value: 'any',
					},
				],
				default: 'outbound',
				description: 'The direction to traverse',
			},
			{
				displayName: 'Max Depth',
				name: 'maxDepth',
				type: 'number',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['traverse'],
					},
				},
				default: 2,
				description: 'Maximum traversal depth',
			},
			{
				displayName: 'Min Depth',
				name: 'minDepth',
				type: 'number',
				displayOptions: {
					show: {
						resource: ['graph'],
						graphOperation: ['traverse'],
					},
				},
				default: 1,
				description: 'Minimum traversal depth',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const credentials = await this.getCredentials('arangoDBCredentialsApi');
		
		const host = credentials.host as string;
		const port = credentials.port as string;
		const username = credentials.username as string;
		const password = credentials.password as string;
		const database = this.getNodeParameter('database', 0) as string;
		
		// Create database connection
		const db = new Database({
			url: `${host}:${port}`,
			auth: { username, password },
			databaseName: database,
		});

		const resource = this.getNodeParameter('resource', 0) as string;

		for (let i = 0; i < items.length; i++) {
			try {
				let responseData: any;

				if (resource === 'document') {
					const operation = this.getNodeParameter('documentOperation', i) as string;
					const collection = this.getNodeParameter('collection', i, '') as string;

					switch (operation) {
						case 'create': {
							const documentData = this.getNodeParameter('documentData', i) as string;
							const returnNew = this.getNodeParameter('returnNew', i) as boolean;
							const data = JSON.parse(documentData);
							
							const result = await db.collection(collection).save(data, {
								returnNew,
							});
							responseData = result;
							break;
						}

						case 'get': {
							const documentKey = this.getNodeParameter('documentKey', i) as string;
							responseData = await db.collection(collection).document(documentKey);
							break;
						}

						case 'getMany': {
							const limit = this.getNodeParameter('limit', i) as number;
							const cursor = await db.query(aql`
								FOR doc IN ${db.collection(collection)}
								LIMIT ${limit}
								RETURN doc
							`);
							responseData = await cursor.all();
							break;
						}

						case 'update': {
							const documentKey = this.getNodeParameter('documentKey', i) as string;
							const documentData = this.getNodeParameter('documentData', i) as string;
							const returnNew = this.getNodeParameter('returnNew', i) as boolean;
							const data = JSON.parse(documentData);
							
							const result = await db.collection(collection).update(documentKey, data, {
								returnNew,
							});
							responseData = result;
							break;
						}

						case 'replace': {
							const documentKey = this.getNodeParameter('documentKey', i) as string;
							const documentData = this.getNodeParameter('documentData', i) as string;
							const returnNew = this.getNodeParameter('returnNew', i) as boolean;
							const data = JSON.parse(documentData);
							
							const result = await db.collection(collection).replace(documentKey, data, {
								returnNew,
							});
							responseData = result;
							break;
						}

						case 'delete': {
							const documentKey = this.getNodeParameter('documentKey', i) as string;
							const result = await db.collection(collection).remove(documentKey);
							responseData = result;
							break;
						}

						case 'query': {
							const aqlQuery = this.getNodeParameter('aqlQuery', i) as string;
							const queryParameters = this.getNodeParameter('queryParameters', i) as string;
							const params = JSON.parse(queryParameters);
							
							const cursor = await db.query(aqlQuery, params);
							responseData = await cursor.all();
							break;
						}
					}
				} else if (resource === 'vectorSearch') {
					const operation = this.getNodeParameter('vectorOperation', i) as string;
					const vectorCollection = this.getNodeParameter('vectorCollection', i) as string;

					switch (operation) {
						case 'search': {
							const queryVector = this.getNodeParameter('queryVector', i) as string;
							const vectorField = this.getNodeParameter('vectorField', i) as string;
							const topK = this.getNodeParameter('topK', i) as number;
							const vector = JSON.parse(queryVector);
							
							// Perform vector similarity search using AQL
							const cursor = await db.query(aql`
								FOR doc IN ${db.collection(vectorCollection)}
								LET distance = DISTANCE(doc.${vectorField}, ${vector})
								SORT distance ASC
								LIMIT ${topK}
								RETURN MERGE(doc, { _distance: distance })
							`);
							responseData = await cursor.all();
							break;
						}

						case 'createIndex': {
							const indexName = this.getNodeParameter('indexName', i) as string;
							const vectorField = this.getNodeParameter('vectorField', i) as string;
							const dimensions = this.getNodeParameter('dimensions', i) as number;
							const distanceMetric = this.getNodeParameter('distanceMetric', i) as string;
							
							// For vector search, we need to use a persistent index with computed values
							// or store vectors as arrays and use AQL for similarity search
							// Since ArangoDB doesn't have native vector index type, we'll create a persistent index
							// and implement vector search using AQL
							const result = await db.collection(vectorCollection).ensureIndex({
								type: 'persistent',
								name: indexName,
								fields: [vectorField],
								unique: false,
								sparse: true,
							});
							
							// Store metadata about vector configuration
							await db.collection('_vector_metadata').save({
								indexName,
								collection: vectorCollection,
								field: vectorField,
								dimensions,
								distanceMetric,
							}, { overwriteMode: 'replace' });
							
							responseData = result;
							break;
						}

						case 'deleteIndex': {
							const indexName = this.getNodeParameter('indexName', i) as string;
							const result = await db.collection(vectorCollection).dropIndex(indexName);
							responseData = { success: result };
							break;
						}
					}
				} else if (resource === 'graph') {
					const operation = this.getNodeParameter('graphOperation', i) as string;
					const graphName = this.getNodeParameter('graphName', i) as string;

					switch (operation) {
						case 'createGraph': {
							const vertexCollections = this.getNodeParameter('vertexCollections', i) as string;
							const edgeDefinitions = this.getNodeParameter('edgeDefinitions', i) as string;
							const vertexArray = vertexCollections.split(',').map(v => v.trim()).filter(v => v);
							const edgeDefs = JSON.parse(edgeDefinitions);
							
							const graph = db.graph(graphName);
							const result = await graph.create(edgeDefs, {
								orphanCollections: vertexArray,
							});
							responseData = result;
							break;
						}

						case 'deleteGraph': {
							const graph = db.graph(graphName);
							const result = await graph.drop();
							responseData = { success: result };
							break;
						}

						case 'addVertex': {
							const vertexCollection = this.getNodeParameter('vertexCollection', i) as string;
							const vertexData = this.getNodeParameter('vertexData', i) as string;
							const data = JSON.parse(vertexData);
							
							const graph = db.graph(graphName);
							const collection = graph.vertexCollection(vertexCollection);
							const result = await collection.save(data);
							responseData = result;
							break;
						}

						case 'addEdge': {
							const edgeCollection = this.getNodeParameter('edgeCollection', i) as string;
							const edgeData = this.getNodeParameter('edgeData', i) as string;
							const data = JSON.parse(edgeData);
							
							const graph = db.graph(graphName);
							const collection = graph.edgeCollection(edgeCollection);
							const result = await collection.save(data);
							responseData = result;
							break;
						}

						case 'traverse': {
							const startVertex = this.getNodeParameter('startVertex', i) as string;
							const direction = this.getNodeParameter('direction', i) as string;
							const minDepth = this.getNodeParameter('minDepth', i) as number;
							const maxDepth = this.getNodeParameter('maxDepth', i) as number;
							
							const cursor = await db.query(aql`
								FOR v, e, p IN ${minDepth}..${maxDepth} ${direction} ${startVertex}
								GRAPH ${graphName}
								RETURN { vertex: v, edge: e, path: p }
							`);
							responseData = await cursor.all();
							break;
						}

						case 'shortestPath': {
							const startVertex = this.getNodeParameter('startVertex', i) as string;
							const endVertex = this.getNodeParameter('endVertex', i) as string;
							
							const cursor = await db.query(aql`
								FOR path IN SHORTEST_PATH ${startVertex} TO ${endVertex}
								GRAPH ${graphName}
								RETURN path
							`);
							responseData = await cursor.all();
							break;
						}

						case 'getNeighbors': {
							const startVertex = this.getNodeParameter('startVertex', i) as string;
							const direction = this.getNodeParameter('direction', i) as string;
							
							const cursor = await db.query(aql`
								FOR v IN 1..1 ${direction} ${startVertex}
								GRAPH ${graphName}
								RETURN v
							`);
							responseData = await cursor.all();
							break;
						}
					}
				} else if (resource === 'custom') {
					const operation = this.getNodeParameter('customOperation', i) as string;
				
					switch (operation) {
						case 'executeAql': {
							const customAqlQuery = this.getNodeParameter('customAqlQuery', i) as string;
							const bindVariables = this.getNodeParameter('bindVariables', i) as string;
							const returnCount = this.getNodeParameter('returnCount', i) as boolean;
							const batchSize = this.getNodeParameter('batchSize', i) as number;
							const variables = JSON.parse(bindVariables);
							
							const options: any = { batchSize };
							if (returnCount) {
								options.count = true;
							}
							
							const cursor = await db.query(customAqlQuery, variables, options);
							responseData = await cursor.all();
							
							if (returnCount) {
								responseData = {
									results: responseData,
									count: cursor.count,
								};
							}
							break;
						}
				
						case 'runTransaction': {
							const transactionCollections = this.getNodeParameter('transactionCollections', i) as string;
							const transactionAction = this.getNodeParameter('transactionAction', i) as string;
							const bindVariables = this.getNodeParameter('bindVariables', i) as string;
							const collections = JSON.parse(transactionCollections);
							const params = JSON.parse(bindVariables);
							
							responseData = await db.executeTransaction(
								collections,
								transactionAction,
								params
							);
							break;
						}
				
						case 'bulkOperation': {
							const bulkOperations = this.getNodeParameter('bulkOperations', i) as string;
							const operations = JSON.parse(bulkOperations);
							const results = [];
							
							for (const op of operations) {
								let result;
								const collection = db.collection(op.collection);
								
								switch (op.operation) {
									case 'insert':
										result = await collection.import(op.data, {
											type: 'documents',
											onDuplicate: op.onDuplicate || 'error',
										});
										break;
									case 'update':
										// Update multiple documents using AQL
										const updateCursor = await db.query(aql`
											FOR doc IN ${collection}
											FILTER MATCHES(doc, ${op.example})
											UPDATE doc WITH ${op.newData} IN ${collection}
											OPTIONS { keepNull: ${op.keepNull || true} }
											RETURN NEW
										`);
										result = await updateCursor.all();
										break;
									case 'remove':
										// Remove multiple documents using AQL
										const removeCursor = await db.query(aql`
											FOR doc IN ${collection}
											FILTER MATCHES(doc, ${op.example})
											REMOVE doc IN ${collection}
											OPTIONS { waitForSync: ${op.waitForSync || false} }
											RETURN OLD
										`);
										result = await removeCursor.all();
										break;
									case 'replace':
										// Replace multiple documents using AQL
										const replaceCursor = await db.query(aql`
											FOR doc IN ${collection}
											FILTER MATCHES(doc, ${op.example})
											REPLACE doc WITH ${op.newData} IN ${collection}
											OPTIONS { waitForSync: ${op.waitForSync || false} }
											RETURN NEW
										`);
										result = await replaceCursor.all();
										break;
									default:
										throw new NodeApiError(
											this.getNode(),
											{error: `Unknown bulk operation: ${op.operation}`},
											{ itemIndex: i },
										);
								}
								results.push({
									operation: op.operation,
									collection: op.collection,
									result,
								});
							}
							responseData = results;
							break;
						}
					}
				}

				// Handle response data
				if (Array.isArray(responseData)) {
					returnData.push(...responseData.map(data => ({
						json: data,
						pairedItem: i,
					})));
				} else {
					returnData.push({
						json: responseData,
						pairedItem: i,
					});
				}
			} catch (error) {
				if (this.continueOnFail()) {
					returnData.push({
						json: { error: error.message },
						error,
						pairedItem: i,
					});
					continue;
				}
				throw new NodeOperationError(this.getNode(), error, {
					itemIndex: i,
				});
			}
		}

		return [returnData];
	}
}