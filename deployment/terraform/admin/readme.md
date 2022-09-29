# Description

Create an admin instance in the OpenStack infrastructure.

The objective of this instance is to allow to do manual and automatic actions to
operate the infrastructure. This instance is also used for infrastructure
automatic creation (using terraform, etc.).

It contains:
* Everything needed to interact with OpenStack infrastructure, i.e.:
    - OpenStack software is installed.
    - The `OS_*` environment variables must be configurable for the current
      project for automation account (using the usual `.sh` file).
    - The password for the automation account is already be present in the
      instance.
    - All the `OS_*` environment variables are set on login.
* Ability to access all buckets (eodata and CoSIMS buckets).
* Some ops tools: Docker, Terraform, Nomad and Packer.

# Usage

To log into the admin instance:

``` shell
terraform output -json admin_external_private_key | jq -r '' > admin_id_rsa
chmod 600 admin_id_rsa
ADMIN_IP_ADDRESS=$(terraform output -json admin_ip_address | jq -r '.')
ssh -i admin_id_rsa eouser@$ADMIN_IP_ADDRESS
```

Sometime the IP address has already been associated to an old instance (admin or
other) from a previous infrstructure creation and your local configartion can
have stored it in your SSH known host files. Then you can see this message when
executing ssh (in this example the ops instance IP address is 45.130.28.32):

```
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
@    WARNING: REMOTE HOST IDENTIFICATION HAS CHANGED!     @
@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@
IT IS POSSIBLE THAT SOMEONE IS DOING SOMETHING NASTY!
Someone could be eavesdropping on you right now (man-in-the-middle attack)!
It is also possible that a host key has just been changed.
The fingerprint for the ECDSA key sent by the remote host is
SHA256:/98s6gG5ZeOcOltaoDjsoIOG4WqWGSPnAyOesT8jdlI.
Please contact your system administrator.
Add correct host key in /home/foo/.ssh/known_hosts to get rid of this message.
Offending ECDSA key in /home/foo/.ssh/known_hosts:45
  remove with:
  ssh-keygen -f "/home/foo/.ssh/known_hosts" -R "45.130.28.32"
ECDSA host key for 45.130.28.32 has changed and you have requested strict checking.
Host key verification failed.
```

In that case simply execute the folowing command then try again the ssh command:

```
ssh-keygen -f "/home/foo/.ssh/known_hosts" -R "$ADMIN_IP_ADDRESS"
```
