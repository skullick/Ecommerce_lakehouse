import os
import sys
import requests


class PolarisSetup:
    def __init__(self):
        self.host = os.environ["POLARIS_HOST"]
        self.port = os.environ["POLARIS_PORT"]
        self.base_url = f"http://{self.host}:{self.port}"

        self.realm = os.environ["REALM_NAME"]
        self.client_id = os.environ["CLIENT_ID"]
        self.client_secret = os.environ["CLIENT_SECRET"]
        self.token = None

        self.minio_host = os.environ["MINIO_HOST"]
        self.minio_port = os.environ["MINIO_PORT"]
        self.minio_bucket = os.environ["MINIO_BUCKET"]

        # Set name for entities and privileges
        self.principal_name = "de_user"
        self.user_client_id = None
        self.user_client_secret = None
        self.catalog_name = os.environ["CATALOG_NAME"]
        self.principal_role_name = "data_engineer_role"
        self.catalog_role_name = "catalog_admin_role"
        self.privileges = ["CATALOG_MANAGE_CONTENT"]

    # ------------------------------------------------------------------ #
    #  HTTP helpers                                                        #
    # ------------------------------------------------------------------ #

    @property
    def _headers(self) -> dict:
        headers = {
            "Accept": "application/json",
            "Polaris-Realm": self.realm,
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        return headers

    def _request(self, method: str, path: str, data=None, form=None) -> dict:
        url = f"{self.base_url}{path}"
        resp = requests.request(
            method,
            url,
            headers=self._headers,
            json=data,
            data=form,
        )
        try:
            resp.raise_for_status()
        except requests.HTTPError as e:
            print("❌ API Error:")
            print(f"URL: {url}")
            print(f"Status: {resp.status_code}")
            print(f"Response: {resp.text}")
            raise e

        return resp.json() if resp.content else {}

    # ------------------------------------------------------------------ #
    #  Setup steps                                                         #
    # ------------------------------------------------------------------ #

    def obtain_token(self):
        print(f"Obtaining root access token of realm '{self.realm}'...")
        resp = self._request(
            "POST",
            "/api/catalog/v1/oauth/tokens",
            form={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "PRINCIPAL_ROLE:ALL",
            },
        )
        self.token = resp.get("access_token")
        if not self.token:
            raise RuntimeError(f"Failed to parse access_token from: {resp}")
        print("✅ Obtained access token")


    def create_catalog(self):
        print(f"Creating catalog '{self.catalog_name}' in realm '{self.realm}'...")
        self._request(
            "POST",
            "/api/management/v1/catalogs",
            data={
                "catalog": {
                    "name": self.catalog_name,
                    "type": "INTERNAL",
                    "readOnly": False,
                    "properties": {
                        "default-base-location": f"s3://{self.minio_bucket}"
                    },
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": [f"s3://{self.minio_bucket}"],
                        "endpoint": f"http://{self.minio_host}:{self.minio_port}",
                        "endpointInternal": f"http://{self.minio_host}:{self.minio_port}",
                        "pathStyleAccess": True,
                    },
                }
            },
        )
        print("✅ Catalog created")


    def create_principal(self):
        print(f"Creating principal '{self.principal_name}' in realm '{self.realm}'...")
        resp = self._request(
            "POST",
            "/api/management/v1/principals",
            data={"principal": {"name": self.principal_name, "properties": {}}},
        )
        creds = resp.get("credentials", {})
        self.user_client_id = creds.get("clientId")
        self.user_client_secret = creds.get("clientSecret")
        if not self.user_client_id or not self.user_client_secret:
            raise RuntimeError(f"Failed to parse user credentials from: {resp}")
        print(f"✅ Principal created with clientId: {self.user_client_id}")


    def create_principal_role(self):
        print(f"Creating principal role '{self.principal_role_name}' in realm '{self.realm}'...")
        self._request(
            "POST",
            "/api/management/v1/principal-roles",
            data={"principalRole": {"name": self.principal_role_name, "properties": {}}},
        )
        print("✅ Principal role created")


    def create_catalog_role(self):
        print(f"Creating catalog role '{self.catalog_role_name}' in realm '{self.realm}'...")
        self._request(
            "POST",
            f"/api/management/v1/catalogs/{self.catalog_name}/catalog-roles",
            data={"catalogRole": {"name": self.catalog_role_name, "properties": {}}},
        )
        print("✅ Catalog role created")


    def assign_principal_role(self):
        print(f"Assigning '{self.principal_role_name}' to principal '{self.principal_name}'...")
        self._request(
            "PUT",
            f"/api/management/v1/principals/{self.principal_name}/principal-roles",
            data={"principalRole": {"name": self.principal_role_name}},
        )
        print("✅ Principal role assigned")


    def assign_catalog_role(self):
        print(f"Assigning '{self.catalog_role_name}' to principal role '{self.principal_role_name}'...")
        self._request(
            "PUT",
            f"/api/management/v1/principal-roles/{self.principal_role_name}/catalog-roles/{self.catalog_name}",
            data={"catalogRole": {"name": self.catalog_role_name}},
        )
        print("✅ Catalog role assigned")


    def grant_privileges(self):
        print(f"Granting CATALOG_MANAGE_CONTENT to '{self.catalog_role_name}'...")
        for p in self.privileges:
            self._request(
                "PUT",
                f"/api/management/v1/catalogs/{self.catalog_name}/catalog-roles/{self.catalog_role_name}/grants",
                data={"type": "catalog", "privilege": p},
            )
        print(f"✅ Privileges {p} granted")

    # ------------------------------------------------------------------ #
    #  Entrypoint                                                          #
    # ------------------------------------------------------------------ #

    def run(self):
        self.obtain_token()
        self.create_catalog()
        self.create_principal()
        self.create_principal_role()
        self.create_catalog_role()
        self.assign_principal_role()
        self.assign_catalog_role()
        self.grant_privileges()

        print("✅ Polaris setup complete.")
        self._write_credentials()

    def _write_credentials(self):
        path = os.environ.get("CREDENTIALS_FILE", "/credentials/polaris-user.env")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w") as f:
            f.write(f"USER_CLIENT_ID={self.user_client_id}\n")
            f.write(f"USER_CLIENT_SECRET={self.user_client_secret}\n")
        print(f"✅ Credentials written to {path}")


if __name__ == "__main__":
    try:
        PolarisSetup().run()
    except Exception as exc:
        print(f"❌ {exc}", file=sys.stderr)
        sys.exit(1)