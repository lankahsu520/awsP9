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
#https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
#pip3 install boto3
#pip3 install --target ./python boto3

import boto3
import botocore
import json

# dynamodb
from boto3.dynamodb.conditions import Key, Attr

from pythonX9 import *

AWS_SERVICE_S3="s3"
AWS_SERVICE_DYNAMODB="dynamodb"
REGION_US_WEST_1='us-west-1'

class awsp9basic(pythonX9):

	def __init__(self, aws_service=[AWS_SERVICE_S3, AWS_SERVICE_DYNAMODB], region=REGION_US_WEST_1, **kwargs):
		if ( isPYTHON(PYTHON_V3) ):
			super().__init__(**kwargs)
		else:
			super(awsp9basic, self).__init__(**kwargs)

		self.region = region
		self.aws_service = aws_service
		DBG_IF_LN(self, "(region: {}, aws_service: {})".format( region, aws_service ))
