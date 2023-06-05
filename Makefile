export

commands : run \
		deploy \
		tidy \

.PHONY: commands

run:
	python app.py

deploy:
	python deploy.py

black:
	black --line-length 79 .

isort:
	isort .

tidy: black
tidy: isort