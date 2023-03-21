from metabase_api import Metabase_API
from credentials import USERNAME, DOMAIN, PASSWORD

mtb = Metabase_API(
    DOMAIN,
    USERNAME,
    PASSWORD
)
