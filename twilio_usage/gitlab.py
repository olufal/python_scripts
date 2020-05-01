"""
Author: Brian Moore

python gitlab.py --project_id 2023 --commit_message minor --working_branch master
Final version
"""
import json
import re
from collections import OrderedDict
import argparse

import requests
import pandas as pd

BASE_ENDPOINT = "http://git.homeadvisor.com/api/v4/projects/"

VERSION = OrderedDict({"major": 2, "minor": 1, "patch": 0})

GIT_HELPER = "Used only for the CI-CD process"


class WriteFile:
    """
    Manages the IO operation
    """

    def __init__(self, name: str):
        """
        Used to manage writing files

        :param name: Name of the said file
        """
        self.name = name

    def __enter__(self):
        self.file = open(self.name, "w")
        return self.file

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.file:
            self.file.close()


class ApiCall:
    """
    Used to close the API request
    """

    def __init__(self, url: str, headerz: dict):
        """
        Forces close on API call

        :param url:
        :param headerz:
        """
        self.url = url
        self.headerz = headerz

    def __enter__(self):
        self.response = requests.get(self.url, headers=self.headerz)
        text = self.response.text
        cargo_load = json.loads(text)
        return cargo_load

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.response:
            self.response.close()


class SemanticVersion:
    """
    Class regex compile code.
    """

    version_re = re.compile(
        r"^(\d+)\.(\d+)\.(\d+)(?:-([0-9a-zA-Z.-]+))?(?:\+([0-9a-zA-Z.-]+))?$"
    )

    def __init__(
            self,
            version: str = None,
            major: str = None,
            minor: str = None,
            patch: str = None,
    ):
        """
        Class to house state and relevant meta data around the semantic version

        :param version:
        :param major:
        :param minor:
        :param patch:
        """
        has_text = version is not None
        valid_version = version == re.sub("[^0-9.]", "", version)
        if has_text and valid_version:
            match = SemanticVersion.version_re.match(version)
            self.major = match.groups()[0]
            self.minor = match.groups()[1]
            self.patch = match.groups()[2]
            self.version = version
        else:
            self.major = str(major) or "0"
            self.minor = str(minor) or "0"
            self.patch = str(patch) or "0"
            self.version = self.construct_version()

    def construct_version(self):
        """
        Pieces together the version

        :return:
        """
        return f"{self.major}.{self.minor}.{self.patch}"

    def increment_version(self, version_type: str) -> str:
        """
        Increments whatever the current version is on the class

        :param version_type:
        :return:
        """

        if version_type in VERSION:
            if version_type == "major":
                major = str(int(self.major) + 1)
                incremented_version = f"{major}.0.0"
            elif version_type == "minor":
                minor = str(int(self.minor) + 1)
                incremented_version = f"{self.major}.{minor}.0"
            elif version_type == "patch":
                patch = str(int(self.patch) + 1)
                incremented_version = f"{self.major}.{self.minor}.{patch}"
            else:
                raise ValueError(f"Failed to account for {version_type}")
            return incremented_version

        raise ValueError(
            f'The request to incrementally bump "{version_type}" is not a valid option.'
        )


def fetch_registry_id(proj_id: int) -> int:
    """
    Get the registry id for the said project

    :param proj_id:
    :return:
    """
    url_string = f"{BASE_ENDPOINT}{proj_id}/registry/repositories"
    with ApiCall(url_string, HEADERS) as api_link:
        data = api_link
    records = len(data)
    if records == 0:
        reg_id = -1
    elif records == 1:
        reg_id = data[0]["id"]
    else:
        raise ValueError(
            'There are multiple registries te'
        )

    return reg_id


def parse_version(record: str) -> str:
    """
    If the image has been versioned it will be grabbed

    :param record:
    :return:
    """

    temp = ""
    if record.find("-") > -1:
        s = record[record.rfind("-") + 1:]
        test = re.sub("[^0-9.]", "", s)
        if s == test:
            temp = s.strip()

    return temp


def fetch_registry_images(proj_id: int, reg_id: int) -> pd.DataFrame:
    """
    Return a tabular data set that lists all columns

    'name' = The tag placed on the image
    'path' = The group/project:tag
    'location' = The fully qualified image name
    'version' = The version the tag is on
    'major' = The extracted major version
    'minor' = The extracted minor version
    'patch' = The extracted path version

    :param proj_id:
    :param reg_id:
    :return: 'name', 'path', 'location', 'version', 'major'
             , 'minor' and 'patch' columns in tabular form
    """
    call = f"{BASE_ENDPOINT}{proj_id}/registry/repositories/{reg_id}/tags"
    with ApiCall(call, HEADERS) as api:
        cargo = api
    registry_image_data = pd.DataFrame(cargo)
    if registry_image_data.shape[0] > 0:
        registry_image_data = registry_image_data[["name", "path", "location"]]
        registry_image_data["version"] = registry_image_data["name"].apply(parse_version)
        for version_type in VERSION:
            registry_image_data[version_type] = registry_image_data["version"].apply(
                lambda x: extract_semantic_version_by_type(x, version_type)
            )
    return registry_image_data


def fetch_max_version(data: pd.DataFrame, semantic_version: str) -> int:
    """
    Finds the value to filter the dataframe by.

    :param data:
    :param semantic_version: 'major', 'minor' or 'patch'
    :return: highest integer
    """
    return data[semantic_version].max()


def fetch_latest_version(data: pd.DataFrame, branch: str = "master") -> SemanticVersion:
    """
    Finds out the max semantic version if it exists on the branch

    :param data:
    :param branch:
    :return:
    """
    branch_registry_data = data[data["name"].str.contains(branch)]
    if branch_registry_data.shape[0] > 0:
        branch_registry_data = branch_registry_data[
            branch_registry_data["version"] != ""
            ]
    if branch_registry_data.shape[0] > 0:
        version = ""
        for version_type in VERSION:
            max_version = fetch_max_version(branch_registry_data, version_type)
            version += f"{str(max_version)}."
            branch_registry_data = branch_registry_data[
                branch_registry_data[version_type] == max_version
                ]
        updated_version = version[:-1]
        print(updated_version)
        return SemanticVersion(updated_version)
    return SemanticVersion("0.0.0")


def fetch_version_increment_id(message: str) -> str:
    """
    If major, minor or patch is not listed in the comment
    then default to patch version bump

    :param message: "Changed the memory read type:patch"
    :return: [2, 1 or 0]; Defaults to 0
    """
    for version_type in VERSION:
        if version_type in message.lower():
            return version_type
    return "patch"


def find_nth(haystack: str, needle: str, n: int) -> int:
    """
    Helper method used for finding anchor locations
    in the provided string.

    :param haystack: The string to be searched
    :param needle: The pattern to be matched
    :param n: the nth instance of the pattern
    :return: The anchor index location
    """
    start = haystack.find(needle)
    while start >= 0 and n > 1:
        start = haystack.find(needle, start + len(needle))
        n -= 1
    return start


def extract_semantic_version_by_type(
        semantic_version: str, semantic_version_type: str
) -> str:
    """
    Takes the full semantic version as input and returns the isolated request type

    :param semantic_version: '0.0.1'
    :param semantic_version_type: 'major', 'minor' or 'patch'
    :return: '8'
    """
    if semantic_version_type in VERSION:
        if semantic_version_type == "major":
            # start = 0
            end = find_nth(semantic_version, ".", 1)
            val = semantic_version[:end]
        elif semantic_version_type == "minor":
            start = find_nth(semantic_version, ".", 1) + 1
            end = find_nth(semantic_version, ".", 2)
            val = semantic_version[start:end]
        elif semantic_version_type == "patch":
            start = find_nth(semantic_version, ".", 2) + 1
            # end = len(semantic_version)
            val = semantic_version[start:]
        else:
            raise ValueError(
                "The developer failed to program this version in the conditional"
            )
    else:
        raise ValueError(f'The version "{semantic_version_type}" is not valid!')
    return val


PARSER = argparse.ArgumentParser(
    description="Base code for auto incrementing the versions", prog="gitlab-semantic"
)

PARSER.add_argument(
    "--project_id", help=GIT_HELPER, nargs="+", dest="project_id", metavar="project_id"
)
PARSER.add_argument(
    "--commit_message",
    help=GIT_HELPER,
    nargs="+",
    dest="commit_message",
    metavar="commit_message",
)
PARSER.add_argument(
    "--working_branch",
    help=GIT_HELPER,
    nargs="+",
    dest="working_branch",
    metavar="working_branch",
)
PARSER.add_argument(
    "--token", help=GIT_HELPER, nargs="+", dest="token", metavar="token"
)

if __name__ == "__main__":
    HEADERS = {"PRIVATE-TOKEN": "BC_BhMNedKPE2rFKruVW"}
    ARGS = PARSER.parse_args()
    PROJECT_ID = ARGS.project_id[0]
    print(PROJECT_ID)
    COMMIT_MESSAGE = ARGS.commit_message[0]
    print(COMMIT_MESSAGE)
    WORKING_BRANCH = ARGS.working_branch[0]
    print(WORKING_BRANCH)
    REGISTRY_ID = fetch_registry_id(PROJECT_ID)
    new_version = '0.0.0'
    if REGISTRY_ID > 0:
        df = fetch_registry_images(PROJECT_ID, REGISTRY_ID)
        if df.shape[0] > 0:
            VERSION_BUMP = fetch_version_increment_id(COMMIT_MESSAGE)
            LATEST_VERSION = fetch_latest_version(df, WORKING_BRANCH)
            new_version = LATEST_VERSION.increment_version(VERSION_BUMP)
    with WriteFile("output.txt") as file:
        file.write(new_version)
