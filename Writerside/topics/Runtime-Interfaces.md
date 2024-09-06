# Runtime Interfaces

The classic 'hms-mirror' interface is the command line interface (CLI).  This is the interface that most users have used in the past.  Support for this is still provided but is quickly being replaced by the Web interface.

The CLI interface requires users to define a configuration file that the is used to control connection endpoints, and cluster attributes.  Runtime operations are controlled by command line options that are passed to the application.

The Web interface provides a much more complete experience for users and allows configurations to be built and validated in a more interactive way.  The Web interface is the preferred interface for users who are new to the application and we encourage existing users to adopt this interface as well since it's capabilities and usability a vast improvement over the CLI.

<tip>
We understand that a big part of the attraction to the CLI was its simple commandline interface and feedback UI.  And under many circumstances, the CLI was a natural choice due to security restrictions and port exposure in CDP environments.  However, the Web interface offers a much more robust and user-friendly experience.  See below for some suggestions on how to gain access to the Web Interface in environments with security restrictions.
</tip>  

## Accessing the Web Interface in Secure Environments

Ports for the Web Interface may not be available in secure enviroments.  By default, the Web Interface is available on port '8090'. 

### Option #1: Alternate Port

If '8090' isn't available, you can change the port by adding the following to the service start up command:
    
```bash
hms-mirror --service --server.port=<new_port>
```

### Option #2: SSH Tunnel

If you can SSH into the host where the service is running, you can most likely create a tunnel to the service port that will allow you to access the Web Interface.  Here's an example:

```bash
ssh -L 8090:<remote_host>:8090 <user>@<remote_host>
```

This will create a tunnel from your local machine to the remote host.  Any traffic you send to `localhost:8090` will be forwarded to the remote host's port `8090`.  Once the tunnel is established, you can open a browser and navigate to `http://localhost:8090/hms-mirror` to access the Web Interface.

Here is a good article on SSH Tunnels: [SSH Tunneling](https://www.ssh.com/ssh/tunneling/example)

### Option #3: Dynamic Port Forwarding

Again, this method relies on SSH access to the remote host.  It also requires advanced SOCKS configuration in your browser.  You first create a dynamic port forward with SSH:

```bash
ssh -D <choose_a_port> <user>@<remote_host>
```

Once that tunnel is established, you can configure your browser to use a SOCKS proxy on `localhost:<choose_a_port>`.  This will allow you to access the Web Interface through the tunnel.

To access the Web Interface, you would navigate to `http://<remote_host>:8090/hms-mirror` (SOCKS proxy will handle the routing).



