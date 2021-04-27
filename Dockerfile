FROM jupyter/minimal-notebook

WORKDIR /app

COPY requirements.txt requirements.txt
# Add dependencies
RUN pip install -r requirements.txt

# Code
COPY src/ /app/src/
#COPY data/ data/
COPY main.py /app/main.py
#COPY evaluation.yaml evaluation.yaml
#COPY hyper_para_eval.py hyper_para_eval.py

# Jupyter
EXPOSE 8888

#CMD python main.py && jupyter notebook

ENTRYPOINT ["python", "main.py"]
