# default to sdist target
all: sdist

venv/:
	virtualenv venv --python=python3

venv/bin/pip-sync: venv/
	venv/bin/pip install pip-tools

# update venv to contain exactly the packages in requirements.txt
sdist: venv/bin/pip-sync
	venv/bin/pip-sync requirements.txt
.PHONY: sdist

# update (but do not install) dependencies
update-dependencies: venv/bin/pip-sync
	venv/bin/pip-compile -U
.PHONY: update-dependencies


start-notebook: sdist
	venv/bin/jupyter notebook --notebook-dir=.
.PHONY: start-notebook
