# Commit messages

Use a title (less than 50 characters) and a body for commit messages. General
speaking, follow the seven rules as stated in this
[article](https://chris.beams.io/posts/git-commit/).

Here is a rationale from the git commit manpage:

> Though not required, it’s a good idea to begin the commit message with a
> single short (less than 50 character) line summarizing the change, followed by
> a blank line and then a more thorough description. The text up to the first
> blank line in a commit message is treated as the commit title, and that title
> is used throughout Git. For example, Git-format-patch(1) turns a commit into
> email, and it uses the title on the Subject line and the rest of the commit in
> the body.

# GitLab Flow

We use the [GitLab Flow] as a git development startegy. Mainly, this consists
of:

* The default main branch is master.
* If needed, specific branches are created for specific actions (deployment in production, in test...).
* To work on a new feature:
  * Create an issue for the feature.
  * Create a merge request for it in GitLab which will automatically create an
  associated branch (something like `42-add-this-new-feature`).
  * Check out this branch on your local working directory.
  * Work on the feature with as much commit as needed until the feature is implemented.
  * Push your work.
  * Unset the WIP status in the GitLab page of the merge request.
  * Check the delete branch box and click the merge button.
  * Done.
  * Optionnaly (preferably before the merge) you can ask someone to review the code of the merge request using GitLab.

# Manual merge

If conflicts are detected between your current branch and the master while merging,
you will need to handle it manually. \
To do so you can follow the steps below:

* Make sure the local branch you want to merge is up to date by fetching the latest updates from the origin.
* Switch to the origin master branch with the command: 'git checkout "origin/master"'.
* Merge the desired branch (ex: 42-branch-to-merge) in your local master branch with the command:

    ```shell
    $git merge --no-ff "42-branch-to-merge"
    ```

* Edit the files that have conflicts to choose which part of the code to keep/remove.
* Use the 'git status' command to check which files will be committed/ignored.
* Use the commands 'git add path/to/file' and 'git rm path/to/file' to respectively add or remove a file from the list of committed ones.
* Once you checked that all the files to be committed are clean and known by git,
use the 'git commit' command.
* Usually your local head will be detached from the origin, preventing you from
'pushing' the merge you just completed. The 'git status' command provides you
this information. If you are in such situation follow the steps below:
  * Create a temporary branch with your current modifications using the command:

    ```shell
    git checkout -b temp
    ```

  * Link the temporary branch to the master one with command: 'git branch -f master temp'.
  * Switch to the newly linked local master branch with command: 'git checkout master'.
  * Remove the temporary branch with command: 'git branch -d temp'.
* Finally you can 'push' your merge to the git repository with command: 'git push origin master'.

# Tests

A set of tests has been written to validate the project. The aim is not to cover 100% of the code, but to ensure that critical functionalities, which were identified beforehand, are working properly.

We differentiate several types of tests : **general**, **functional**, **unitary**, each one being positioned at a specific level in the project folder tree :

* **General tests**, located at the project's root folder : they test the services
workflow, interact with the database, to ensure that services are working properly.
They require a database to be up, and involved services to be running
(currently "job_configuration").

* **Functional tests**, located at the components level : each test validate a
specific component functionality, which needs a database to interact with it.
Thus, they require a database to be up while executed.

* **Unit tests**, located at the components level : each test validate a specific
component functionality, which is not dependent on a database. They can be run locally
without any additional component.

A test is represented by a function, with a name prefixed by "test", defined in a
Python file, saved under a "test_*.py" or "*_test.py" name. It should validate a
condition with the "assert" method. Any test can be run manually using the
**PyTest** library, by following the steps below :

* First, make sure that **PyTest** is installed in your Python stack (python3-pytest).
* Open a Linux terminal at the project's root folder.
* Export the **COSIMS_DB_HTTP_API_BASE_URL** environment variable with, as value,
the url path leading to your database (for **General**/**Functional** tests), or
any value you like (for **Unit** tests).
* Then, the syntax to run a specific test "test_1" located in a file called
"test_file.py" is the following one :

> pytest-3 path/to/your/test/test_file.py::test_1

* All the tests located in the same file ("test_file.py" for instance) can be run
one after an other with the following command :

> pytest-3 path/to/your/test/test_file.py

* If just a path is given as argument to the pytest command as shown below,
**PyTest** automatically look for all the files matching the "test_*.py"
and "*_test.py" patterns, and run every test located in the corresponding files.
Note that the files can be located in the specified folder or in subfolders
under the mentioned path.

> pytest-3 path/to/your/test/

* The "*" character can be used in a path to select any element. For instance,
the command below will run tests located in files under
"path/to/your/test/test_file.py" as well as files under
"path/to/my/test/test_file.py".

> pytest-3 path/to/*/test/

* If several tests are run at once, an execution summary will be displayed in
the terminal as they are executed. In this one, passed tests are represented by
a green dot, broken ones by a red "E" letter. Details on the part of the test
which failed are displayed once all the tests have been run.

# GitLab CI

The GitLab CI is a set of steps that are run after every commit to the repository.
These steps are defined in the ".gitlab-ci.yml" file, and currently we proceed the
following ones :  

* We build and push the database docker image.
* We run the tests described above, **Unit** and **Functional**.
* We build and push the orchestrator services docker images.

The **Unit** and **Functional** tests are thuus run automatically by Git after every commit, to validate that no functionalites have been unintentionally impacted by these ones.

The definition of each of the steps listed above in the ".gitlab-ci.yml" file is quite straight forward. Each of the steps is labelled by a flag ("build database docker", "unit tests", ...). Under these tags, a **variable** one is used to define environment variables. If needed, a set of commands defined in the **before_script**/**after_script** label can be run before/after the tests. Finally the command to run the tests should be added under the **script** flag.

# TODO in code

When your need to add a `TODO` in a comment in source code, add a word to tell
the level of importance of the resolution of the `TODO`:

* `MUST` indicates that the system can't be shipped if the `TODO` is not
   adressed.
* `SHOULD` it is important but it is ok to not adressed it.
* `MAY` the resolution is optionnal for now.
