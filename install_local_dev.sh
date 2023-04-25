source <(cat .env | sed 's/^/export /g')

pip install -r requirements.txt
