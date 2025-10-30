pipeline {
    agent any

    environment {
        // Use the credential IDs you created in Jenkins
        DB_HOST = credentials('DATABRICKS_HOST')
        DB_TOKEN = credentials('DATABRICKS')
    }

    stages {
        stage('Checkout Code') {
            steps {
                echo 'Checking out code from Git...'
                // --- UPDATE THIS LINE ---
                git branch: 'main', url: 'https://github.com/rahulchowdary01/databricks-etl-project.git
            }
        }

        stage('Configure Databricks CLI') {
            steps {
                // Configure the CLI using the credentials. 
                // 'set +x' prevents the token from being printed in the logs.
                sh """
                set +x
                echo "Configuring Databricks CLI..."
                databricks configure --token $DB_TOKEN --host $DB_HOST
                set -x
                echo "Configuration complete."
                """
            }
        }

        stage('Deploy Notebooks to Databricks') {
            steps {
                echo "Deploying notebooks to Databricks Workspace..."
                // This command uploads the 'notebooks' folder from your Git repo
                // to the '/Shared/Jenkins_Deploy' folder in your Databricks workspace.
                // The --overwrite flag ensures it updates existing files.
                sh 'databricks workspace import_dir ./notebooks /Shared/Jenkins_Deploy --overwrite'
            }
        }
    }

    post {
        success {
            echo 'CI/CD Pipeline finished successfully!'
        }
        failure {
            echo 'Pipeline failed.'
        }
    }
}
