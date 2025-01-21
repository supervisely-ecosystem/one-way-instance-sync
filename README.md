<div align="center" markdown>
<img src="https://github.com/user-attachments/assets/42c63fc9-7137-4df6-9324-1690f420f0c5" />
  
# One Way Instance Synchronization

<p align="center">
  <a href="#Introduction">Introduction</a> •
  <a href="#How-to-Use">How to Use</a>
</p>

[![](https://img.shields.io/badge/supervisely-ecosystem-brightgreen)](https://ecosystem.supervise.com/apps/supervisely-ecosystem/copy-team-between-instances)
[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.com/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/copy-team-between-instances)
[![views](https://app.supervise.com/img/badges/views/supervisely-ecosystem/copy-team-between-instances)](https://supervise.com)
[![runs](https://app.supervise.com/img/badges/runs/supervisely-ecosystem/copy-team-between-instances)](https://supervise.com)

</div>

# Introduction

Import Supervisely Team from one instance to another. All selected team members and workspaces will be imported.

App can be used **only by master user of the both instances**: current (where you run the app) and the one you want to import from.

</details>

# How to Use

## Step 1. Connect to Instance you want to import from

![connect]()

1. Provide server address

Copy server address from your browser address bar.

![server-address]()

2. Provide API token. You can find it in your profile settings on the instance you want to import from.

Open instance you want to import from in browser and login as master user. Then go to your profile settings.

![profile-settings]()

Open API Token tab and copy your token to the app.

![profile-token]()

3. Press "Connect" button.

![connect-success]()

## Step 2. Select team to import. Press on the "Select" button in the table of the team you want to import.

![team-table]()

## Step 3. Select Entities to import

There are three sections in the app each of them allows you to select and configure specific items that you want to import:

1. Workspaces
2. Team Members

![select-entities]()

### 1. Workspaces

In this section you can select which workspaces you want to import.
If you want to import all workspaces, check "Import all workspaces" checkbox, otherwise you can manually select specific projects in workspaces that you want to import.

![entities-ws]()

If the team you want to import already exists on the current instance, you might find projects in selector that are marked as disabled. It means that these project already exists in selected workspace and these projects will not be imported unless "Remove and reupload projects that already exists" option is selected.

![ws-projects-exists]()

**Workspace import options**

There are a few options to select from when importing workspaces:

1. Data Synchronization Scenarios - what to do if project with the same name already exists in the workspace you want to import to.

    - Skip projects that already exists - ignore projects that already exists on the current instance.
    - Remove and reupload projects that already exists - remove project from current instance and reupload it from the instance you want to import from.

2. Data transfer - select how to import data from another instance based on original upload method.
    - Copy data from instance to instance by reuploading (Slow) - completely reupload all data from another instance. Slow, but safe option.
    - Copy data by links, if possible (Fast) - if your data is linked to cloud storage, these links will be used when transferring data. If those links don't exists anymore, data will not be validated, which can result in data loss. If data is not available by link, it will be reuploaded using default method. Fast but not safe option.

If you want to copy data by links from cloud storage, but you migrated your data to another cloud storage you can use "Change transfer links for items" option. This option requires you to connect to cloud storage you want to use for data transfer.

![change-link-connect]()

Select provider, enter the bucket name and press "Connect" button. If you have successfully connected old cloud storage links will be replaced with new ones using new provider and bucket name.

![change-link-connect-success]()

For example you migrated your data from GCS to AWS **keeping the same folder structure** for your data. You can use this option to replace old GCS links with new AWS links.

```text
 'gcs://my-bucket/data/my_project/image_01.jpg' -> 's3://new-bucket/data/my_project/image_01.jpg'
```

Please notice that only provider and bucket name have changed, but folder structure for image is the same.

### 2. Team Members

In this section you can select which team members you want to import.

You can see the list of team members and their roles on the instance you want to import from. Manually select specific team members that you want to import.

If you see that some users are marked as disabled, it means that team that you want to import already exists on the current instance, and these users are already in this team and exists and they will not be imported. In case their roles are different, you can change them to match roles on the instance you are importing from.

![entities-users]()

If users you want to import are completely new to the current instance, they will be created with the role "annotator" and you need to specify default password for them. Don't forget to notify them about their new password.

## Step 4. Start import

Press "Start Import" button and wait for the app to transfer data between instances, once finished you will see the following message: "Data have been successfully imported.".
You can finish the app or select another team to import.

![import-success]()
