FROM python:3.13

# Install bash
RUN apt-get update && apt-get install -y bash

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["python", "main.py"]