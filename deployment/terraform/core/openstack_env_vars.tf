// Get some OpenStack information from OS_* env vars.
//
// The use of jq is a trick to access external env vars from terraform template.
// The string in the form:
//   "env | { MY_ENV_VAR }"
// is a jq command that output the following JSON:
//   {
//     "MY_ENV_VAR": "the value of MY_ENV_VAR when terraform is launch"
//   }
// And the data.external template takes this JSON and produces a resource that
// can be used like that:
//    data.external.openstack_env_vars.result.MY_ENV_VAR
data "external" "openstack_env_vars" {
    program = [
      "jq",
      "-n",
      "env | {OS_PASSWORD, OS_USERNAME, OS_PROJECT_ID, OS_PROJECT_NAME}"
    ]
}
