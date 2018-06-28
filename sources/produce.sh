if [[ ! -d "./venv" ]]; then
  echo "Virtualenv not found, creating one..."
  virtualenv venv
  source venv/bin/activate
  pip install -r requirements.txt
fi

python producer.py
