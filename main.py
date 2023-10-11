from loaders.postgres_loader import PostgresLoader

if __name__ == '__main__':
    postgres = PostgresLoader(
        db_name="keycloak",
        user="keycloak",
        password="PS?R&aGN4F&BgQ@8",
        host="localhost",
    )

    postgres.load("client", "A dataset about clients")
    print(postgres.save_data())
