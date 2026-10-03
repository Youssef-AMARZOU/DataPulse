FROM apache/airflow:3.3.2-python3.12

COPY requirements.txt /requirements.txt

# Pas de --constraint ici : soda-core est incompatible avec 2 pins des constraints
# officielles 3.3.2 (ruamel.yaml==0.19.1 vs <0.18, requests==2.34.2 vs <2.34).
# Les dépendances Airflow sont déjà installées et épinglées dans l'image de base :
# pip n'ajuste que ce que soda/otel nécessitent.
RUN pip install --no-cache-dir -r /requirements.txt \
 && airflow version \
 && soda --help >/dev/null
