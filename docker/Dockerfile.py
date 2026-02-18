FROM python:3.12

WORKDIR /work

COPY requirements.txt /work/requirements.txt
RUN pip install --no-cache-dir -r /work/requirements.txt

CMD ["sh", "-c", "python3 practices/p2/src/init_validation.py && python3 practices/p2/src/seed_data.py &&  exec sleep infinity"]

# CMD ["bash"]

