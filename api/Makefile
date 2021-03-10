Image=tbutzer/kelvin



build:
	docker build -t ${Image} .

run:
	docker run -it ${Image} bash

analyze:
	pyan3 --dot convert_kelvin_celsius.py | dot -Tpng -o a.png


orchstrate:
	python3 kelvin_orchestration.py
