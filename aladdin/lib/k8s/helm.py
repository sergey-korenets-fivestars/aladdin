#!/usr/bin/env python3
import json
import os
import subprocess

from botocore.exceptions import ClientError
from os.path import join, expanduser

from aladdin.lib import logging

logger = logging.getLogger(__name__)


class Helm(object):

    @property
    def helm_home(self):
        return join(expanduser("~"), ".helm")

    @classmethod
    def repo_exists(cls, s3_bucket, repo_name):
        return bool(list(s3_bucket.objects.filter(Prefix=f"{repo_name}/index.yaml")))

    def publish(self, project_name, publish_rules, chart_path, hash, force=True):

        version = f"1.0+{hash}"

        logger.info("Building package")

        charts_dir, chart_name = os.path.split(chart_path)
        helm_repo_name = f"{project_name}/{chart_name}"
        if not self.repo_exists(publish_rules.s3_bucket, helm_repo_name):
            subprocess.check_call([
                "helm",
                "s3",
                "init",
                f"s3://{publish_rules.s3_bucket.name}/{helm_repo_name}"
            ])

        subprocess.check_call([
            "helm",
            "package",
            "--version",
            version,
            chart_name
        ], cwd=charts_dir)

        extra_push_args = []
        if force:
            extra_push_args.append("--force")

        try:
            subprocess.check_call([
                "helm",
                "repo",
                "add",
                helm_repo_name,
                f"s3://{publish_rules.s3_bucket.name}/{helm_repo_name}"
            ])
            subprocess.check_call([
                "helm",
                "s3",
                "push",
                *extra_push_args,
                "./{}-{}.tgz".format(chart_name, version),
                helm_repo_name,
            ], cwd=charts_dir)
        finally:
            os.remove(package_path)

    def pull_and_extract(self, project_name, chart, publish_rules, git_ref):
        """
        Retrieve all charts published for this project at this git_ref.

        This will download all the published charts and extract them to extract_dir.

        :param project_name: The name of the aladdin project being retrieved.
        :param publish_rules: Details for accessing the S3 bucket.
        :param git_ref: The version of the chart(s) to retrieve.
        :param extract_dir: Where to place the downloaded charts.
        """
        version = f"1.0+{git_ref}"
        package = f"{chart}-{version}.tgz"
        helm_repo_name = f"{project_name}/{chart_name}"
        try:
            subprocess.check_call([
                "helm",
                "pull",
                f"s3://{publish_rules.s3_bucket.name}/{helm_repo_name}/{package}"
            ])
            subprocess.check_call(["tar", "-xvzf", package])
        finally:
            try:
                os.remove(package)
            except FileNotFoundError:
                pass

    def find_values(self, chart_path, cluster_name, namespace):
        """
        Find all possible values yaml files for override in increasing priority
        Values and overrides are defined/specified in helm args in a specific order
        1. project values.yaml (picked up by helm automatically)
        2. project cluster values.yaml
        3. project cluster namespace values.yaml
        4. site.yaml file (on local)
        5. aladdin-config default `values.yaml`
        6. aladdin-config cluster `values.yaml`
        7. aladdin-config cluster namespace `values.yaml`
        8. user passed overrides
        """
        values = []

        cluster_values_path = join(chart_path, "values", f"values.{cluster_name}.yaml")
        if os.path.isfile(cluster_values_path):
            logger.info("Found cluster values file")
            values.append(cluster_values_path)

        cluster_namespace_values_path = join(
            chart_path, "values", f"values.{cluster_name}.{namespace}.yaml"
        )
        if os.path.isfile(cluster_namespace_values_path):
            logger.info("Found cluster namespace values file")
            values.append(cluster_namespace_values_path)

        site_values_path = join(chart_path, "values", "site.yaml")  # Only usable on LOCAL
        if cluster_name == "LOCAL" and os.path.isfile(site_values_path):
            logger.info("Found site values file")
            values.append(site_values_path)

        aladdin_config_values_path = os.path.join(
            os.environ["ALADDIN_CONFIG_DIR"],
            "default",
            "values.yaml"
        )
        if os.path.isfile(aladdin_config_values_path):
            logger.info("Found aladdin config values file")
            values.append(aladdin_config_values_path)

        cluster_config_values_path = os.path.join(
            os.environ["ALADDIN_CONFIG_DIR"],
            cluster_name,
            "values.yaml"
        )
        if os.path.isfile(cluster_config_values_path):
            logger.info("Found cluster config values file")
            values.append(cluster_config_values_path)

        cluster_namespace_config_values_path = os.path.join(
            os.environ["ALADDIN_CONFIG_DIR"],
            cluster_name,
            namespace,
            "values.yaml"
        )
        if os.path.isfile(cluster_namespace_config_values_path):
            logger.info("Found cluster namespace config values file")
            values.append(cluster_namespace_config_values_path)

        return values

    def stop(self, helm_rules, namespace):
        release_name = helm_rules.release_name

        command = ["helm", "delete", release_name, "--namespace", namespace]

        if self.release_exists(release_name, namespace):
            subprocess.run(command, check=True)
            logger.info("Successfully removed release {}".format(release_name))
        else:
            logger.warning(
                "Could not remove release {} because it doesn't exist".format(release_name)
            )

    def release_exists(self, release_name, namespace):
        command = ["helm", "status", release_name, "--namespace", namespace]

        ret_code = subprocess.run(
            command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        ).returncode
        # If return code is 0, release exists
        if ret_code == 0:
            return True
        else:
            return False

    def rollback_relative(self, helm_rules, num_versions, namespace):
        release_name = helm_rules.release_name

        helm_list_command = ["helm", "list", "--namespace", namespace, "-o", "json"]
        output = json.loads(subprocess.run(helm_list_command, capture_output=True).stdout)
        current_revision = int([k["revision"] for k in output if k["name"] == release_name][0])

        if num_versions > current_revision:
            logger.warning("Can't rollback that far")
            return

        self.rollback(helm_rules, current_revision - num_versions, namespace)

    def rollback(self, helm_rules, revision, namespace):
        release_name = helm_rules.release_name
        command = ["helm", "rollback", release_name, str(revision), "--namespace", namespace]
        subprocess.run(command, check=True)

    def start(
        self, helm_rules, chart_path, cluster_name, namespace, force=False, helm_args=None, **values
    ):
        if helm_args is None:
            helm_args = []
        if force:
            helm_args.append("--force")
        logger.info("Installing release %s in namespace %s", helm_rules.release_name, namespace)
        return self._run(helm_rules, chart_path, cluster_name, namespace, helm_args, **values)

    def dry_run(self, helm_rules, chart_path, cluster_name, namespace, helm_args=None, **values):
        if helm_args is None:
            helm_args = []
        helm_args += ["--dry-run", "--debug"]
        logger.info(
            "Dry run installing release %s in namespace %s", helm_rules.release_name, namespace
        )
        return self._run(helm_rules, chart_path, cluster_name, namespace, helm_args, **values)

    def _run(self, helm_rules, chart_path, cluster_name, namespace, helm_args=None, **values):
        release_name = helm_rules.release_name

        command = [
            "helm",
            "upgrade",
            release_name,
            chart_path,
            "--install",
            "--namespace={}".format(namespace),
        ]

        for path in self.find_values(chart_path, cluster_name, namespace):
            command.append("--values={}".format(path))

        for set_name, set_val in values.items():
            command.extend(["--set", "{}={}".format(set_name, set_val)])

        if helm_args:
            command.extend(helm_args)

        logger.info("Executing: %s", " ".join(command))
        subprocess.run(command, check=True)
