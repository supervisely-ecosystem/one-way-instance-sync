<div align="center" markdown>
<img src="https://github.com/user-attachments/assets/65c9e51f-b284-49d1-a321-0d4bce0d06a3" />
  
# One Way Instance Synchronization

<p align="center">
  <a href="#Introduction">Introduction</a> •
  <a href="#How-to-Use">How to Use</a>
</p>

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervise.com/apps/supervisely-ecosystem/one-way-instance-sync)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.com/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/one-way-instance-sync)
[![views](https://app.supervise.com/img/badges/views/supervisely-ecosystem/one-way-instance-sync)](https://supervise.com)
[![runs](https://app.supervise.com/img/badges/runs/supervisely-ecosystem/one-way-instance-sync)](https://supervise.com)

</div>

# Introduction

This application is designed for synchronizing data from one Supervisely instance to another. It allows syncing teams, workspaces, and their contents.

Important: Only a master user (admin) of both instances (the current one and the source instance) can use this application.

</details>

# How to Use

## Step 1: Connect to the Source Instance

1. Enter the server address (URL of the source instance).
2. Provide the API token (found in your profile settings on the source instance).
3. Click the "Connect" button.

Once successfully connected, a list of available teams will be displayed.

## Step 2. Select a Team to Synchronize

Click the "Select" button next to the team you want to synchronize.

## Step 3. Select Entities to Synchronize

### Workspaces

Select the workspaces and projects you want to synchronize.

If you need to sync all workspaces, enable the "Synchronize all workspaces" checkbox. Otherwise, you will need to select the projects to synchronize for each workspace manually.

#### Data Synchronization Options

Choose how to handle existing projects:

-   **Skip existing projects** – Only new projects will be synchronized; existing ones will be ignored.
-   **Download missing and update outdated items** – New entities will be added, and existing ones will be updated if changes are detected.
-   **Remove and reupload existing projects** – If a project already exists, it will be deleted and fully reuploaded from the source instance.

#### Data Transfer Method

-   **Slow** (Re-upload all files) – All data will be re-uploaded, regardless of how it was originally stored.
-   **Fast** (Copy links if possible) – If the data is stored in the cloud, existing links will be used. If links are unavailable, files will be re-uploaded.
    -   **Change Transfer Links** (for migrated cloud storage)<br>
        If your data has been migrated to another cloud storage (e.g., from GCS to AWS), you can update transfer links by selecting the new provider and bucket.<br>
        For example, if you moved your data from GCS to AWS while maintaining the same folder structure, you can use this option to replace old GCS links with new AWS links, ensuring seamless data transfer.

### Team Members

Choose how to handle existing users:

-   **Keep existing roles unchanged** – Users' roles on the current instance will remain the same.
-   **Update roles to match the source team** – Users' roles will be updated to match the source instance.

A default password can be set for newly created users.

## Step 4: Start Synchronization

After configuring all options, click "Start Synchronization" to begin the process.
