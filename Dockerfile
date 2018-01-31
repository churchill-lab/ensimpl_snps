FROM python:3.6
LABEL maintainer="Matthew Vincent <mattjvincent@gmail.com>" \
	  version="1.0"

ENV INSTALL_PATH /app/ensimpl_snps
RUN mkdir -p $INSTALL_PATH

WORKDIR $INSTALL_PATH

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .
RUN pip install --editable .

CMD gunicorn -c "python:config.gunicorn" "ensimpl_snps.app:create_app()"
