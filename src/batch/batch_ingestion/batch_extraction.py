from typing import List
import requests
from requests.auth import HTTPBasicAuth

API_ROOT = 'http://localhost:8000/api/v1'  # This is the default for Airbyte


def get_workspaces() -> List[str]:
    response = requests.post(f'{API_ROOT}/workspaces/list', auth=HTTPBasicAuth('airbyte', 'password'))
    response.raise_for_status()  # Either handle this yourself, or use a tool like Sentry for logging
    return [
        workspace['workspaceId']
        for workspace in response.json()['workspaces']
    ]

def get_connections_for_workspace(workspace_id: str) -> List[str]:
    response = requests.post(
        f'{API_ROOT}/connections/list',
        json={'workspaceId': workspace_id},
        auth=HTTPBasicAuth('airbyte', 'password')
    )
    response.raise_for_status()
    return [
        connection['connectionId']
        for connection in response.json()['connections']
        if connection['status'] == 'active'  # So we can still disable connections in the UI
    ]

def trigger_connection_sync(connection_id: str) -> dict:
    response = requests.post(
        f'{API_ROOT}/connections/sync',
        json={'connectionId': connection_id},
        auth=HTTPBasicAuth('airbyte', 'password')
    )
    response.raise_for_status()
    return response.json()


if __name__ == '__main__':
    workspaces = get_workspaces()
    connections = []
    
    for workspace_id in workspaces:
        connections.extend(get_connections_for_workspace(workspace_id))
       
    for connection_id in connections:
        trigger_connection_sync(connection_id)