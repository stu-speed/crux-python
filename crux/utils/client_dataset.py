"""Module contains ClientDataset object."""

from collections import defaultdict
from typing import (
    DefaultDict,
    Dict,
    IO,
    Iterator,
    List,
    MutableMapping,
    Optional,
    Set,
    Text,
    Tuple,
    Union,
)  # noqa: F401

from crux._client import CruxClient
from crux.exceptions import (
    CruxAPIError,
    CruxResourceNotFoundError,
)
from crux.models import Dataset
from crux.models import Delivery
from crux.models.resource import MediaType


class ClientDataset(Dataset):
    """Client library to retrieve data from Crux."""

    def __init__(self, raw_model=None, connection=None):
        # type: (Dict, CruxClient) -> None
        """
        Attributes:
            raw_model (dict): Resource raw dictionary. Defaults to None.
            connection (CruxClient): Connection object. Defaults to None.
        """

        self._frames = {}  # type: Dict
        self._latest_delivery = None  # type: Delivery
        self._latest_raw = defaultdict(dict)  # type: DefaultDict[str, Dict]
        self._latest_frames = defaultdict(dict)  # type: DefaultDict[str, Dict]

        super(ClientDataset, self).__init__(raw_model=raw_model, connection=connection)

        self.refresh()

    @property
    def properties(self):
        """str: Gets the Dataset raw model attributes."""
        return self.raw_model

    @property
    def frames(self):
        """str: Gets the latest set of frames."""
        if not self._frames:
            for frame_id, frame in self._latest_frames.items():
                raw_resource = self._latest_raw[frame[MediaType.AVRO.value][0].provenance["raw_resource_id"]]
                remote_path = raw_resource.provenance.get("extractor_conf", {}).get("remote_path", "Unknown")
                self._frames[frame_id] = {"vendor_file_path": remote_path + raw_resource.name}
        return self._frames

    def refresh(self):
        """Refresh attributes from API backend."""

        self._frames.clear()
        self._latest_raw.clear()
        self._latest_frames.clear()

        latest_ingestion = self.get_latest_ingestion()
        if latest_ingestion:
            self._latest_delivery = self.get_delivery(
                "{}.{}".format(latest_ingestion.id, max(latest_ingestion.versions))
            )
            for resource in self._latest_delivery.get_raw():
                self._latest_raw[resource.id] = resource
            for resource in self._latest_delivery.get_data(MediaType.AVRO.value):
                if self._latest_frames.get(resource.labels["frame_id"], {}).get(MediaType.AVRO.value, None):
                    self._latest_frames[resource.labels["frame_id"]][MediaType.AVRO.value].append(resource)
                else:
                    self._latest_frames[resource.labels["frame_id"]][MediaType.AVRO.value] = [resource]
        else:
            raise CruxResourceNotFoundError({"statusCode": 901, "message": "Latest ingestion for dataset not found"})
