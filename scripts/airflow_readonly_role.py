"""Create / refresh a READ-ONLY Airflow role that sees only some DAGs (by name prefix).

The role = Airflow's built-in "Viewer" permissions MINUS "read all DAGs",
PLUS "read" on each DAG whose id starts with the prefix. So a user with this
role can open those DAGs, their runs, task logs and code — but can't trigger,
clear, pause or edit anything, and doesn't see any other client's DAGs.

Run it INSIDE the Airflow webserver container (the script is piped in, nothing
needs to be mounted). On the VM, from ~/etl:
    docker compose --env-file .env -f docker/docker-compose.prod.yml exec -T airflow-webserver \
        python - --role HangyoViewer --prefix hangyo_ < scripts/airflow_readonly_role.py
Add --dry-run to only print what it would do.

Re-run it after new hangyo_* DAGs are added (or DAGs renamed) — it adds the
missing ones and removes DAGs that no longer exist. Then create the user:
    ... exec airflow-webserver airflow users create --username <name> --role HangyoViewer \
        --firstname <first> --lastname <last> --email <email>      (asks for the password)
"""
import argparse

from airflow.models import DagModel
from airflow.security import permissions as P
from airflow.utils.session import create_session
from airflow.www.app import cached_app

ap = argparse.ArgumentParser()
ap.add_argument("--role", required=True, help="role name, e.g. HangyoViewer")
ap.add_argument("--prefix", required=True, help="DAG id prefix, e.g. hangyo_")
ap.add_argument("--dry-run", action="store_true")
a = ap.parse_args()

# never copy these from Viewer: they mean "every DAG"
ALL_DAGS = {(P.ACTION_CAN_READ, P.RESOURCE_DAG), (P.ACTION_CAN_EDIT, P.RESOURCE_DAG),
            (P.ACTION_CAN_DELETE, P.RESOURCE_DAG), (P.ACTION_CAN_CREATE, P.RESOURCE_DAG_RUN),
            (P.ACTION_CAN_EDIT, P.RESOURCE_DAG_RUN), (P.ACTION_CAN_DELETE, P.RESOURCE_DAG_RUN)}

with create_session() as session:
    dag_ids = sorted(d for (d,) in session.query(DagModel.dag_id)
                     .filter(DagModel.dag_id.startswith(a.prefix), DagModel.is_active.is_(True)))
if not dag_ids:
    raise SystemExit(f"no active DAGs start with '{a.prefix}'")

app = cached_app()
with app.app_context():
    sm = app.appbuilder.sm
    viewer = sm.find_role("Viewer")
    want = {(p.action.name, p.resource.name) for p in viewer.permissions} - ALL_DAGS
    for d in dag_ids:
        want.add((P.ACTION_CAN_READ, P.resource_name_for_dag(d)))                 # "DAG:<id>"
        want.add((P.ACTION_CAN_READ, P.resource_name(d, P.RESOURCE_DAG_RUN)))     # "DAG Run:<id>"

    role = sm.find_role(a.role)
    have = {(p.action.name, p.resource.name): p for p in (role.permissions if role else [])}
    add = sorted(want - have.keys())
    drop = sorted(have.keys() - want)

    print(f"role {a.role} ({'exists' if role else 'NEW'}) - {len(dag_ids)} DAGs readable:")
    for d in dag_ids:
        print("   ", d)
    print(f"permissions: {len(want)} total, +{len(add)} to add, -{len(drop)} to remove")
    for x in drop:
        print("    remove", x)
    if a.dry_run:
        raise SystemExit("dry run - nothing changed")

    role = role or sm.add_role(a.role)
    for action, resource in add:
        sm.add_permission_to_role(role, sm.get_permission(action, resource) or sm.create_permission(action, resource))
    for key in drop:
        sm.remove_permission_from_role(role, have[key])
    print(f"done - role {a.role} is read-only on {len(dag_ids)} '{a.prefix}*' DAGs")
