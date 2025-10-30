pipeline {
    agent any

    environment {
        // Use the credential IDs you created in Jenkins
        DB_HOST = credentials('DATABRICKS_HOST')
        DB_TOKEN = credentials('DATABRICKS_TOKEN')
    }

    stages {
        stage('Checkout Code') {
            steps {
                echo 'Checking out code from Git...'
                // --- UPDATE THIS LINE ---
                git branch: 'main', url: 'https://github.com/rahulchowdary01/databricks-etl-project.git'
            }
        }

        stage('Configure Databricks CLI') {
            steps {
                // This creates the config file manually for the 'jenkins' user
                // at its home directory: /var/lib/jenkins/
                sh """
                set +x
                echo "Configuring Databricks CLI..."
                echo "[DEFAULT]" > /var/lib/jenkins/.databrickscfg
                echo "host = $DB_HOST" >> /var/lib/jenkins/.databrickscfg
                echo "token = $DB_TOKEN" >> /var/lib/jenkins/.databrickscfg
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
