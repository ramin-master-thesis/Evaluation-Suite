FROM python:3.8.5-slim-buster AS compile-image

RUN apt-get update \
&& apt-get install -y --no-install-recommends build-essential gcc

RUN python -m venv /opt/venv
# Make sure we use the virtualenv:
ENV PATH="/opt/venv/bin:$PATH"

COPY requirements.txt .

RUN pip install -r requirements.txt

FROM python:3.8.5-slim-buster AS build-image
COPY --from=compile-image /opt/venv /app
WORKDIR /app

# Make sure we use the virtualenv:
ENV PATH="/app/bin:$PATH"

# Code
COPY src/ /app/src/
COPY main.py /app/main.py

# Jupyter
EXPOSE 8888

#CMD python main.py && jupyter notebook

ENTRYPOINT ["python", "main.py"]
