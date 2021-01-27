all: zip

zip:
	GIT_VERSION="$(shell git describe --abbrev=6 --always --tags)"
	DIR=$(CURDIR)
	cd $(DIR)
	rm -rf projects/dqr*.zip
	zip -r projects/dqr.zip  dqr/ setup.py README.md LICENSE  -x "**/__pycache__/*" "**/.git/**" "**/*_out"
