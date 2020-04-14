FROM python:3.7

COPY . .
WORKDIR .

RUN pip install civis
RUN pip install pandas
RUN pip install --upgrade google-api-python-client
RUN pip install google-auth-httplib2
RUN pip install google-auth-oauthlib

WORKDIR .

CMD /bin/bash -c "python myc_dedupe.py"
