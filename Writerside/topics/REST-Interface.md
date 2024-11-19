# REST Interface (Technical Preview)

Start `hms-mirror` with the `--service` option.
    
```bash
hms-mirror --service
```

<warning>
This feature is in technical preview and is subject to change. It is not complete and may not be fully functional.
</warning>

The REST Swagger documentation is available at `http://server-host:8090/hms-mirror/swagger-ui/index.html`.

The REST base endpoint is `http://server-host:8090/hms-mirror/api/v1`.

The REST service controls the 'current' session in `hms-mirror`.  This session is the same as the session available 
through the Web Interface.