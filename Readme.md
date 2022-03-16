## Preparation

Create pytyhon virtualenv and install requirements

```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

Install offline language translation model
```bash
wget "https://argosopentech.nyc3.digitaloceanspaces.com/argospm/translate-de_en-1_0.argosmodel" -O "lib/translate-de_en-1_0.argosmodel"
```

## Versions

Tested with there versions:
java: openjdk version "11.0.12" 2021-07-20
spark: 3.2.1
python: 3.9.10
python direct dependencies: pandas openpyxl pyspark pyarrow argostranslate

## Run Whole pipeline

```bash
python src/spark_pipeline.py -a
```

If you want to run some particular part of pipeline
```bash
python src/spark_pipeline.py -s <stage>
```

## Assumptions & Comments

**Assumptions**

- In production, data is not stored in Excel file, and instead in some database. Excel has some implicit type cоnversions, so I decided to not believe these types, but instead convert everything to string on integration part. Therefore all `null` values are stored as `"null"` strings.
- There are mappings of known colors in database, and all other colors are converted to `Other`. I decided to use this mapping (recreate it from data, that I have), by preprocessing TargetData. This is done once before running pipeline, and result are saved to json files.
Alternatively, I could just say, that Bordeaux and Antracite are commn colors, and to not change the to `Other`, if they don't violate any constraints.
- Common thing is happenning with make of the car. But here is another problem. There are no other simple solutions to the problem of how to distinguish abbreviations from words, and therefore, capitalize whole word or just first letter. Mapping from database also helps here.
I also consider, that there are not too much makes existing, and they all can be placed in memory. This is probably true for makes and colors, but there are cases, where in is not true.
In that cases, right way is to store that file somewhere and join this it into spark dataframe.
- I used pandas to save csv files. Saving spark dataframe directly will save different partitions separretly & store some metadata, but for simplicity, I decided to just store in single csv file. In production, of course, sending all data to one node may not be acceptable.
- Since every city in supplier data is from Switzerland, and there is not price provided, I decided to to put `currency=CHF` and `price_on_request=True`. I could not leave these fields nullable, as they are not nullable in given Excel file, which may be database constraint

**Comments**
- I'm using argostranslate to translate colors from German to English. This in not the best model, but one of the lightest free offline models I found. And Of course there os no data race in sending it to different nodes, as it is not used as mutable.
- The same with vehicle type. I put car everywhere just to make it not nullable, but actually it looked like there are soome trucks. Anyway, I do not now what exactly could be written there except car.
- This language model does not work great with for example Body Types of vehicles or Conditions. Neither do Google traslate, so I decided not to hardcode translations, since I am not sure in those, and it is not the best practive.
- I did not try to make real pipeline using pipleline launching systems. Just made simple python script, which may be integrated with just a bit changes to some popular systems.
