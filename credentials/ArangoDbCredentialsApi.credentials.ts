import {
	IAuthenticateGeneric,
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class ArangoDbCredentialsApi implements ICredentialType {
	name = 'arangoDBCredentialsApi';
	displayName = 'ArangoDB Credentials API';
	documentationUrl = 'https://www.arangodb.com/docs/stable/http/';
	properties: INodeProperties[] = [
		{
			displayName: 'Host',
			name: 'host',
			type: 'string',
			default: 'http://localhost',
			placeholder: 'http://localhost',
			required: true,
		},
		{
			displayName: 'Port',
			name: 'port',
			type: 'number',
			default: 8529,
			required: true,
		},
		{
			displayName: 'Username',
			name: 'username',
			type: 'string',
			default: '',
			required: true,
		},
		{
			displayName: 'Password',
			name: 'password',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			required: true,
		},
	];

	authenticate: IAuthenticateGeneric = {
		type: 'generic',
		properties: {
			headers: {
				Authorization: '=Basic {{Buffer.from(credentials.username + ":" + credentials.password).toString("base64")}}',
			},
		},
	};
} 