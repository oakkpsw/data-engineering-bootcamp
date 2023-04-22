#!/bin/bash
#parntry ID a172d577-a967-4fbe-906a-ec88153f1abc
curl -XPOST -H "Content-type: application/json" -d @dogs.json "https://getpantry.cloud/apiv1/pantry/a172d577-a967-4fbe-906a-ec88153f1abc/basket/randomDogs"