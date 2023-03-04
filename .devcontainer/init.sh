sudo apt update
sudo apt install postgresql postgresql-contrib -y
sudo cp ./.devcontainer/pg_hba.conf /etc/postgresql/12/main/pg_hba.conf
sudo chown postgres:postgres /etc/postgresql/12/main/pg_hba.conf
sudo service postgresql start

sudo psql -U postgres -c "ALTER USER postgres PASSWORD 'password';"

dotnet restore