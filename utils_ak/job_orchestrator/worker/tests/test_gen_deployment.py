from utils_ak.job_orchestrator.worker.gen_deployment import create_deployment


def test_gen_deployment():
    print(
        create_deployment(
            "<container_name>",
            "<deployment_id>",
            "<payload>",
            "<image>",
            "<python_main>",
        )
    )


if __name__ == "__main__":
    test_gen_deployment()
