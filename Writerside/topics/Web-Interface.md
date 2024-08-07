# Web Interface

## Starting

The `hms-mirror` web application is started by running:

```bash
hms-mirror --service
```

This will start the web application on the default port of `8090`.  The port can be changed by using the `--service.port` option during startup.

```bash
hms-mirror --service --service.port=8080
```

Point your browser to `http://server-host:8090/hms-mirror` to access the web application.

The web application will use the user home directory to store configuration files, logs, and reports.  This isn't as much a concern as it is for the `cli` version since the web application will manage all this from the user's browser.

The web application works on the premise of a 'session'.  Although the application is stateless, the session is used to store the user's configuration and state while they are using the application.  This allows the user to navigate the application and make changes without losing their work.  Currently, the session is global throughout the application and is not tied to a specific user browser session.  An 'hms-mirror' session is NOT synonymous with a browser session.

An instance of the web application can only run ONE session at a time.

## Stopping

`hms-mirror --stop`

## Security

Coming Soon...

## Where to Start?

There are three methods to building/loading a configuration.  From the main page, select 'Initialize'.

![web_init_menu](web_init_menu.png)

This will bring up the 'Initialize' page where one of the following options can be selected.

![web_init.png](web_init.png)

> Configurations are specific to a [Data Strategy](Strategies.md). Once created, you can NOT change the Data 
> Strategy but, you can clone the configuration and change the Data Strategy (last option).

### Pick an Existing Configuration

Load a previously saved configuration.  Changes can be made and saved back to the same file or a new file.

### Create a New Configuration

Create a new configuration from scratch.

### Clone an Existing Configuration

Using an existing configuration, clone it and change the Data Strategy.  This will allow you to maintain any 
previously configured endpoints to Hive Server 2 and Metastore Direct connections.

## Managing the Session Configuration

There are 2 states for a session.  The in-memory state and the persisted state.

When you choose to 'Edit' a session configuration, you'll need to 'Save' the configuration for those changes to be 
applied to the session.  'Saving' the configuration will ONLY update the in-memory state of the session.  

![session_mngt.png](session_mngt.png)

You will 
need to 'Persist' the session to save it for future use.