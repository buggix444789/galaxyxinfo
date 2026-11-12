FROM python:3.10

WORKDIR /code

COPY ./requirements.txt /code/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

COPY . .

# 'sh -c' ka use karke pinger aur uvicorn dono ko ek saath chala rahe hain
CMD ["sh", "-c", "python pinger.py & uvicorn app:app --host 0.0.0.0 --port 7860"]
