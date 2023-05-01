# -*- coding: utf-8 -*-
"""
 ***************************************************************************
 * Copyright (C) 2023, Lanka Hsu, <lankahsu@gmail.com>, et al.
 *
 * This software is licensed as described in the file COPYING, which
 * you should have received as part of this distribution.
 *
 * You may opt to use, copy, modify, merge, publish, distribute and/or sell
 * copies of the Software, and permit persons to whom the Software is
 * furnished to do so, under the terms of the COPYING file.
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY
 * KIND, either express or implied.
 *
 ***************************************************************************
"""
from awsp9basic import *

class awsp9DB(awsp9basic):

	# A low-level client representing Amazon DynamoDB
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html
	#dydb_create_table(TableName="Music", PK="Artist", SK="SongTitle")
	def dydb_create_table(self, TableName="", PK="", SK=""):
		self.dydb_response = ""
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			else:
				AttributeDefinitions= [
					{'AttributeName': PK,'AttributeType': 'S'},
					{'AttributeName': SK,'AttributeType': 'S'}
				]
				KeySchema=[
					{'AttributeName': PK,'KeyType': 'HASH'},
					{'AttributeName': SK,'KeyType': 'RANGE'}
				]
				self.dydb_response = self.dbcli.create_table(
					AttributeDefinitions=AttributeDefinitions,
					TableName=TableName,
					KeySchema=KeySchema,
					ProvisionedThroughput={
						'ReadCapacityUnits': 5,
						'WriteCapacityUnits': 5
					},
				)
			self.dydb_describe_table(TableName=TableName, status="CREATING")
			DBG_DB_LN(self, "{} (TableName: {}, PK: {}, SK: {})". format( DBG_TXT_DONE, TableName, PK, SK ) )
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_delete_table(self, TableName=""):
		self.dydb_response = ""
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			else:
				self.dydb_describe_table(TableName=TableName, status="ACTIVE")
				self.dydb_response = self.dbcli.delete_table(TableName=TableName)
				retry = 10
				while (retry>0) and (self.dydb_describe_table(TableName=TableName) != ""):
					sleep(400/1000)
					retry-=1
				DBG_DB_LN(self, "{} (TableName: {})". format( DBG_TXT_DONE, TableName ) )
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_describe_table(self, TableName="", status=""):
		self.dydb_response = ""
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			else:
				self.dydb_response = self.dbcli.describe_table(TableName=TableName)
				#DBG_WN_LN(self, "{}".format(self.dydb_response) )
				if ( status != "" ):
					retry = 10
					while (retry>0) and (self.dydb_response['Table']['TableStatus'] == status):
						self.dydb_response = self.dydb_describe_table(TableName=TableName)
						sleep(400/1000)
						retry-=1
				DBG_DB_LN(self, "{} (TableName: {})". format( DBG_TXT_DONE, TableName ) )
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_list_tables(self, StartTableName="", limit=100):
		self.dydb_response =[]
		try:
			if (StartTableName == ""):
				self.dydb_response = self.dbcli.list_tables(Limit=limit)
			else:
				self.dydb_response = self.dbcli.list_tables(ExclusiveStartTableName=StartTableName, Limit=limit)
			DBG_DB_LN(self, "{} (StartTableName: {}, limit: {})". format( DBG_TXT_DONE, StartTableName, limit ) )
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, StartTableName: {})".format( e.__str__(), error_code, StartTableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, StartTableName: {})".format( e.__str__(), error_code, StartTableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_del_item(self, TableName=""):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			elif ( len(self.keyX) == 0 ):
				DBG_ER_LN(self, "self.keyX is Null !!!" )
			else:
				self.dydb_response = self.dbcli.delete_item(
						Key=self.keyX,
						TableName=TableName
					)
				DBG_DB_LN(self, "{} (TableName: {}, keyX: {})". format( DBG_TXT_DONE, TableName, self.keyX ) )
				self.dydb_keyX_free()
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_get_item(self, TableName=""):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			elif ( len(self.keyX) == 0 ):
				DBG_ER_LN(self, "self.keyX is Null !!!" )
			else:
				self.dydb_response = self.dbcli.get_item(
						Key=self.keyX,
						TableName=TableName
					)
				DBG_DB_LN(self, "{} (TableName: {}, keyX: {})". format( DBG_TXT_DONE, TableName, self.keyX ) )
				self.dydb_keyX_free()
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_put_item(self, TableName=""):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			elif ( len(self.attrX) == 0 ):
				DBG_ER_LN(self, "self.attrX is Null !!!" )
			else:
				self.dydb_response = self.dbcli.put_item(
						Item=self.attrX,
						ReturnConsumedCapacity='TOTAL',
						TableName=TableName
					)
				DBG_DB_LN(self, "{} (TableName: {}, attrX: {})". format( DBG_TXT_DONE, TableName, self.attrX ) )
				self.dydb_attrX_free()
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_query_item(self, TableName="", **kwargs):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			else:
				tableX = self.dbsrc.Table(TableName)
				self.dydb_response = tableX.query(**kwargs)
				DBG_DB_LN(self, "{} (TableName: {})". format( DBG_TXT_DONE, TableName ) )
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	# class boto3.dynamodb.conditions.Attr(name)[source]
	# https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/dynamodb.html#boto3.dynamodb.conditions.Attr
	def dydb_scan_item(self, TableName="", **kwargs):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			else:
				tableX = self.dbsrc.Table(TableName)
				self.dydb_response = tableX.scan(**kwargs)
				DBG_DB_LN(self, "{} (TableName: {})". format( DBG_TXT_DONE, TableName ) )
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_update_item(self, TableName=""):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			elif ( len(self.attrX) == 0 ):
				DBG_ER_LN(self, "self.attrX is Null !!!" )
			else:
				d = self.attrX
				s = ", ".join([f'{k}=:{k}' for k, v in d.items()])
				SET_STR = "SET {}".format(s)
				s = ", ".join([f'\':{k}\': {v}' for k, v in d.items()])
				EXPRESS_STR = f"{{ {s} }}"
				EXPRESS_DICT = json.loads(EXPRESS_STR.replace("'", "\""))
				DBG_TR_LN(self, "(EXPRESS_STR: {})". format( EXPRESS_STR ) )
				DBG_TR_LN(self, "(SET_STR: {})". format( SET_STR ) )
				self.dydb_response = self.dbcli.update_item(
						Key=self.keyX,
						ReturnValues='ALL_NEW',
						ExpressionAttributeValues=EXPRESS_DICT,
						UpdateExpression=SET_STR,
						TableName=TableName
					)
				DBG_DB_LN(self, "{} (TableName: {}, attrX: {})". format( DBG_TXT_DONE, TableName, self.attrX ) )
				self.dydb_keyX_free()
				self.dydb_attrX_free()
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	def dydb_remove_attributes(self, TableName="", attributes=""):
		self.dydb_response =[]
		try:
			if (TableName == ""):
				DBG_ER_LN(self, "TableName is Null !!!" )
			elif ( len(attributes) == 0 ):
				DBG_ER_LN(self, "attributes is Null !!!" )
			else:
				SET_STR = "REMOVE {}".format( attributes )
				DBG_TR_LN(self, "(SET_STR: {})". format( SET_STR ) )
				self.dydb_response = self.dbcli.update_item(
						Key=self.keyX,
						UpdateExpression=SET_STR,
						TableName=TableName
					)
				DBG_DB_LN(self, "{} (TableName: {}, attributes: {})". format( DBG_TXT_DONE, TableName, attributes ) )
				self.dydb_keyX_free()
				self.dydb_attrX_free()
		except botocore.exceptions.ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		except ClientError as e:
			error_code = e.response['Error']['Code']
			DBG_ER_LN(self, "{} (error_code:{}, TableName: {})".format( e.__str__(), error_code, TableName ))
			self.dydb_error_code = error_code
		return self.dydb_response

	# String
	def dydb_keyX_addS(self, key="", value=""):
		valueDict={ "S": value }
		self.keyX.update( { key: valueDict } )
		DBG_TR_LN(self, "(keyX: {})".format( self.keyX ))

	def dydb_keyX_free(self):
		self.keyX = {}

	def dydb_attrX_update(self, value={}):
		self.attrX.update( value )
		DBG_TR_LN(self, "(attrX: {})".format( self.attrX ))

	def dydb_attrX_addX_with_dict(self, value={}):
		self.attrX.update( value )
		DBG_TR_LN(self, "(attrX: {})".format( self.attrX ))

	# String
	def dydb_attrX_addS(self, key="", value=""):
		valueDict={ "S": value }
		self.attrX.update( { key: valueDict } )
		DBG_TR_LN(self, "(attrX: {})".format( self.attrX ))

	# Number
	def dydb_attrX_addN(self, key="", value=0):
		valueDict={ "N": "{}".format(value) }
		self.attrX.update( { key: valueDict } )
		DBG_TR_LN(self, "(attrX: {})".format( self.attrX ))

	# Boolean
	def dydb_attrX_addBoolean(self, key="", value=True):
		valueDict={ "BOOL": value }
		self.attrX.update( { key: valueDict } )
		DBG_TR_LN(self, "(attrX: {})".format( self.attrX ))

	# List
	def dydb_attrX_addListS(self, key="", value="", separator=":"):
		valueL= [{"S": s} for s in value.split(separator)]
		valueDict={ "L": valueL }
		self.attrX.update( { key: valueDict } )
		DBG_TR_LN(self, "(attrX: {})".format( self.attrX ))

	def dydb_attrX_free(self):
		self.attrX = {}

	def release(self):
		self.is_quit = 1

	def __init__(self, aws_service=[AWS_SERVICE_DYNAMODB], region=REGION_US_WEST_1, **kwargs):
		if ( isPYTHON(PYTHON_V3) ):
			super().__init__(aws_service, region, **kwargs)
		else:
			super(awsp9DB, self).__init__(aws_service, region, **kwargs)

		self.attrX = {}
		self.keyX = {}

		DBG_IF_LN(self, "(region: {}, aws_service: {})".format( region, aws_service ))
		for service in aws_service:
			if ( service == "dynamodb" ):
				self.dbsrc = boto3.resource("dynamodb")
				self.dbcli = boto3.client("dynamodb")
