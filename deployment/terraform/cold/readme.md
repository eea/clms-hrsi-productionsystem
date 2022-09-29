This module contains resources managed by Terraform that don't change often
and/or that must be protected from accidental deletion.

For example, this contains the resource needed to define the public ssh key
that is used as a GitLab deploy token. Its public key is set in the GitLab
interface manualy and needs to be stable.