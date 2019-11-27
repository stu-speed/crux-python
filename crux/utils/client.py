"""Module contains CruxClient Class library."""

from datetime import datetime, timedelta
import os

from crux.apis import Crux
from crux.utils.client_dataset import ClientDataset
from crux._utils import create_logger

logger = create_logger(__name__)

class Client(Crux):
    """Client library to retrieve data from Crux."""

    def __init__(
        self,
        api_key=None,  # type: Optional[str]
        api_host=None,  # type: str
    ):
        super(Client, self).__init__(api_key=api_key, api_host=api_host)

    def get_dataset(self, id):  # type: (str) -> ClientDataset
        dsobj = super(Client, self).get_dataset(id)
        return ClientDataset(raw_model=dsobj.raw_model, connection=dsobj.connection)
