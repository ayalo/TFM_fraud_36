.PHONY: deploy

deploy:
	#zip -r source/utils.zip source/utils -x  source/utils/__pycache__/*
	rsync -avz source/utils.zip  ogarcia@tardigrado.netcom.it.uc3m.es:/home/ogarcia/deploy/

get:
	rsync -avz ogarcia@tardigrado.netcom.it.uc3m.es:/home/ogarcia/deploy/notebooks source/