#!/usr/bin/env python3
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

#import os, sys, errno, getopt, signal, time, io
#from time import sleep

from awsp9 import *

REGION_NAME=REGION_US_WEST_1

#aws_service = [ AWS_SERVICE_DYNAMODB, AWS_SERVICE_S3 ]
aws_service = [ AWS_SERVICE_S3 ]
app_list = []
is_quit = 0
app_apps = {
}

def demo_s3(awsP9_mgr):
	DBG_DB_LN("{}".format(DBG_TXT_ENTER))
	awsP9_mgr.s3_check_bucket("lambdax9")
	DBG_IF_LN("(s3_response: {})".format(awsP9_mgr.s3_response))
	DBG_IF_LN("(s3_error_code: {})".format(awsP9_mgr.s3_error_code))

	awsP9_mgr.s3_get_bucket_location("lambdax9")
	DBG_IF_LN("(s3_response: {})".format(awsP9_mgr.s3_response))
	DBG_IF_LN("(s3_error_code: {})".format(awsP9_mgr.s3_error_code))

def app_start():
	#dbg_lvl_set(DBG_LVL_DEBUG)
	awsP9_mgr = awsp9(aws_service=aws_service, region=REGION_NAME, dbg_more=DBG_LVL_DEBUG)

	app_watch(awsP9_mgr)

	demo_s3(awsP9_mgr)

def app_watch(app_ctx):
	global app_list

	app_list.append( app_ctx )

def app_release():
	global app_list

	DBG_DB_LN("{}".format(DBG_TXT_ENTER))
	for x in app_list:
		try:
			objname = DBG_NAME(x)
			if not x.release is None:
				DBG_DB_LN("call {}.release ...".format( objname ) )
				x.release() # No handlers could be found for logger "google.api_core.bidi"
		except Exception:
			pass
	DBG_DB_LN("{}".format(DBG_TXT_DONE))

def app_stop():
	global is_quit

	# dont block this function or print, signal_handler->app_stop
	if ( is_quit == 0 ):
		is_quit = 1

def app_exit():
	app_stop()
	app_release()
	DBG_DB_LN("{}".format(DBG_TXT_DONE))

def show_usage(argv):
	print("Usage: {} <options...>".format(argv[0]) )
	print("  -h, --help")
	print("  -d, --debug level")
	print("    0: critical, 1: errror, 2: warning, 3: info, 4: debug, 5: trace")
	app_exit()
	sys.exit(0)

def parse_arg(argv):
	global app_apps

	try:
		opts,args = getopt.getopt(argv[1:], "hd:", ["help", "debug"])
	except getopt.GetoptError:
		show_usage(argv)

	#print (opts)
	#print (args)

	if (len(opts) > 0):
		for opt, arg in opts:
			if opt in ("-h", "--help"):
				show_usage(argv)
			elif opt in ("-d", "--debug"):
				dbg_debug_helper( int(arg) )
			else:
				print ("(opt: {})".format(opt))
	else:
		show_usage(argv)

def signal_handler(sig, frame):
	if sig in (signal.SIGINT, signal.SIGTERM):
		app_stop()
		return
	sys.exit(0)

def main(argv):
	global is_quit
	global app_apps

	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)

	parse_arg(argv)

	app_start()

	app_exit()
	DBG_WN_LN("{} (is_quit: {})".format(DBG_TXT_BYE_BYE, is_quit))

if __name__ == "__main__":
	main(sys.argv[0:])
