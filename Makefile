PWD=$(shell pwd)
-include $(SDK_CONFIG_CONFIG)

#** include *.mk **
-include define.mk

#[major].[minor].[revision].[build]
VERSION_MAJOR = 1
VERSION_MINOR = 0
VERSION_REVISION = 0
VERSION_FULL = $(VERSION_MAJOR).$(VERSION_MINOR).$(VERSION_REVISION)
LIBNAME_A = xxx
LIBNAME_SO =
LIBNAME_MOD =

#** GITHUB_LIBS **
GITHUB_LIBS = \
														https://github.com/lankahsu520/pythonX9.git

#** PYTHON_FILES **
PYTHON_FILES = \
														awsp9-db_123.py \
														awsp9-s3_123.py

DEBUG=3
DEBUG_ARG=-d $(DEBUG)

#********************************************************************************
#** All **
#********************************************************************************

.DEFAULT_GOAL = all

.PHONY: all clean distclean layer_python
all: $(PYTHON_FILES)

clean:
	$(PJ_SH_RM) export.log
	$(PJ_SH_RM) .configured
	$(PJ_SH_RMDIR) __pycache__/ ./python/ github_libs/
	$(PJ_SH_RM) $(PJ_NAME)/version.txt
	@for subdir in $(CONFS_yes); do \
		[ -d "$$subdir" ] && (make -C $$subdir $@;) || echo "skip !!! ($$subdir)"; \
	done

distclean: clean

layer_python:
	@echo '----->> $@ - $(PWD)/python'
	@if [ ! -d "$(PWD)/python" ]; then \
		(pip3 install --target $(PWD)/python -r requirements.txt); \
		for libs in $(GITHUB_LIBS); do (git clone $$libs github_libs && $(PJ_SH_CP) github_libs/*.py $(PWD)/python && rm -rf github_libs); done \
	fi
	@echo

$(PYTHON_FILES): layer_python
	@echo
	@echo '----->> run $@'
	PYTHONPATH=$(PWD)/python ./$@ $(DEBUG_ARG)
