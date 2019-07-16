.PHONY: deploy

deploy:
	rsync -avz source/*    ogarcia@tardigrado.netcom.it.uc3m.es:/home/ogarcia/deploy/

#get:
#	rsync -avz ogarcia@tardigrado.netcom.it.uc3m.es:/home/ogarcia/deploy/notebooks source/