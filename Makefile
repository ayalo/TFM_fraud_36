.PHONY: deploy

deploy:
	rsync -avz source/*    ogarcia@tardigrado.netcom.it.uc3m.es:/home/ogarcia/deploy/
