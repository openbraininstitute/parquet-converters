pipeline {
    agent { label 'bb5' } 
    parameters {
        string(name: 'GERRIT_REFSPEC', defaultValue: 'refs/heads/master', description: 'What to build', )
    }
    environment {
        PACKAGE = "parquet-converters"
        PACKAGE_URL = "ssh://bbpcode.epfl.ch/building/ParquetConverters"
        PACKAGE_SPEC = "%gcc@4.8.5 ^zlib~shared ^hdf5@1.10:"
        PACKAGE_VERSION = "develop"

        DATADIR = "/gpfs/bbp.cscs.ch/project/proj12/jenkins/"

        HOME = "${WORKSPACE}/BUILD_HOME"
        SOFTS_DIR_PATH = "${WORKSPACE}/INSTALL_HOME"
        SPACK_ROOT = "${HOME}/spack"
        PATH = "${SPACK_ROOT}/bin:${PATH}"
    }

    stages {
        stage('Checkout') {
            steps {
                git url: "${PACKAGE_URL}"
            }
        }
        stage('Change Branch') {
            when {
                expression {
                    return env.GERRIT_REFSPEC.matches("^refs/changes/.*")
                }
            }
            steps {
                script {
                    def changeBranch = "change-${GERRIT_CHANGE_NUMBER}-${GERRIT_PATCHSET_NUMBER}"
                    sh "git fetch origin ${GERRIT_REFSPEC}:${changeBranch}"
                    sh "git checkout ${changeBranch}"
                }
            }
        }
        stage('Build') {
            steps {
                dir(HOME + '/.spack') {
                    deleteDir()
                }
                dir(SPACK_ROOT) {
                    git(url: "https://github.com/BlueBrain/spack.git", branch: "develop")
                    dir(SPACK_ROOT + '/etc/spack/defaults/linux') {
                        sh "cp ${SPACK_ROOT}/sysconfig/bb5/users/* ."
                    }
                    sh """
                        sed -i "s#git=url#git='file://${WORKSPACE}'#g" \
                            ${SPACK_ROOT}/var/spack/repos/builtin/packages/${PACKAGE}/package.py"""
                    sh """
                        unset \$(env | awk -F= '/^PMI_/ {print \$1}' | xargs)
                        source ${SPACK_ROOT}/share/spack/setup-env.sh
                        spack install -j ${env.SLURM_CPUS_PER_TASK} ${PACKAGE}@${PACKAGE_VERSION} ${PACKAGE_SPEC}
                    """
                }
            }
        }
        stage('Test') {
            steps {
                script {
                    def tasks = [:]
                    // def files = findFiles(glob: ".jenkins/test_*.sh")
                    // FIXME WORKAROUND FOR LINE ABOVE
                    sh "find .jenkins -type f -name test_\\*.sh > tests_to_execute"
                    def files = readFile("tests_to_execute").split()
                    // For every test in .jenkins, we create a new shell
                    // script that ensures that the environment is loaded
                    // correctly.
                    def header = "#!/bin/sh\n" +
                                 "unset \$(env | awk -F= '/^SLURM_/ {print \$1}' | xargs)\n" +
                                 "source ${SPACK_ROOT}/share/spack/setup-env.sh\n" +
                                 "spack load ${PACKAGE}@${PACKAGE_VERSION}${PACKAGE_SPEC}\n" +
                                 "set -xe\n"
                    for (f in files) {
                        def name = f.split("/")[-1].replaceAll(/^test_/, "").replaceAll(/\.sh$/, "")
                        def fullpath = WORKSPACE + '/' + f
                        def testpath = WORKSPACE + "/tests_to_execute_" + name

                        writeFile(file: testpath, text: header + readFile(fullpath))

                        tasks[name] = {
                            stage(name) {
                                dir(name) {
                                    sh "chmod +x " + testpath
                                    sh testpath
                                }
                            }
                        }
                    }
                    parallel tasks
                }
            }
        }
    }
    post {
        always {
            cleanWs()
        }
    }
}
