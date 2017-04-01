package pipelines_test

import (
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"code.cloudfoundry.org/lager"
	"code.cloudfoundry.org/lager/lagertest"

	"github.com/concourse/go-concourse/concourse"
	"github.com/concourse/testflight/helpers"
	"github.com/mgutz/ansi"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"

	"testing"
	"time"

	"github.com/nu7hatch/gouuid"
)

var (
	client concourse.Client
	team   concourse.Team

	flyBin string

	pipelineName string

	tmpHome string
	logger  lager.Logger
)

var atcURL = helpers.AtcURL()
var targetedConcourse = "testflight"

var _ = SynchronizedBeforeSuite(func() []byte {
	Eventually(helpers.ErrorPolling(atcURL)).ShouldNot(HaveOccurred())

	data, err := helpers.FirstNodeFlySetup(atcURL, targetedConcourse)
	Expect(err).NotTo(HaveOccurred())

	return data
}, func(data []byte) {
	var err error
	flyBin, tmpHome, err = helpers.AllNodeFlySetup(data)
	Expect(err).NotTo(HaveOccurred())

	client, err = helpers.AllNodeClientSetup(data)
	Expect(err).NotTo(HaveOccurred())

	team = client.Team("main")
	logger = lagertest.NewTestLogger("pipelines-test")
})

var _ = SynchronizedAfterSuite(func() {
}, func() {
	os.RemoveAll(tmpHome)
})

var _ = BeforeEach(func() {
	guid, err := uuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	pipelineName = fmt.Sprintf("test-pipeline-%d-%s", GinkgoParallelNode(), guid)
})

var _ = AfterEach(func() {
	destroyPipeline(pipelineName)
})

func TestGitPipeline(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Pipelines Suite")
}

func destroyPipeline(name string) {
	destroyCmd := exec.Command(
		flyBin,
		"-t", targetedConcourse,
		"destroy-pipeline",
		"-p", name,
		"-n",
	)

	destroy, err := gexec.Start(destroyCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	<-destroy.Exited

	if destroy.ExitCode() == 1 {
		if strings.Contains(string(destroy.Err.Contents()), "does not exist") {
			return
		}
	}

	Expect(destroy).To(gexec.Exit(0))
}

func renamePipeline() {
	guid, err := uuid.NewV4()
	Expect(err).ToNot(HaveOccurred())

	newName := fmt.Sprintf("test-pipeline-%d-renamed-%s", GinkgoParallelNode(), guid)

	renameCmd := exec.Command(
		flyBin,
		"-t", targetedConcourse,
		"rename-pipeline",
		"-o", pipelineName,
		"-n", newName,
	)

	rename, err := gexec.Start(renameCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	<-rename.Exited
	Expect(rename).To(gexec.Exit(0))

	pipelineName = newName
}

func configurePipeline(argv ...string) {
	destroyPipeline(pipelineName)

	reconfigurePipeline(argv...)
}

func reconfigurePipeline(argv ...string) {
	args := append([]string{
		"-t", targetedConcourse,
		"set-pipeline",
		"-p", pipelineName,
		"-n",
	}, argv...)

	configureCmd := exec.Command(flyBin, args...)

	configure, err := gexec.Start(configureCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	<-configure.Exited
	Expect(configure.ExitCode()).To(Equal(0))

	unpausePipeline()
}

func pausePipeline() {
	pauseCmd := exec.Command(flyBin, "-t", targetedConcourse, "pause-pipeline", "-p", pipelineName)

	configure, err := gexec.Start(pauseCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	<-configure.Exited
	Expect(configure.ExitCode()).To(Equal(0))

	Expect(configure).To(gbytes.Say("paused '%s'", pipelineName))
}

func unpausePipeline() {
	unpauseCmd := exec.Command(flyBin, "-t", targetedConcourse, "unpause-pipeline", "-p", pipelineName)

	configure, err := gexec.Start(unpauseCmd, GinkgoWriter, GinkgoWriter)
	Expect(err).NotTo(HaveOccurred())

	<-configure.Exited
	Expect(configure.ExitCode()).To(Equal(0))

	Expect(configure).To(gbytes.Say("unpaused '%s'", pipelineName))
}

func flyWatch(jobName string, buildName ...string) *gexec.Session {
	args := []string{
		"-t", targetedConcourse,
		"watch",
		"-j", pipelineName + "/" + jobName,
	}

	if len(buildName) > 0 {
		args = append(args, "-b", buildName[0])
	}

	keepPollingCheck := regexp.MustCompile("job has no builds|build not found|failed to get build")
	for {
		session := start(exec.Command(flyBin, args...))

		<-session.Exited

		if session.ExitCode() == 1 {
			output := strings.TrimSpace(string(session.Err.Contents()))
			if keepPollingCheck.MatchString(output) {
				// build hasn't started yet; keep polling
				time.Sleep(time.Second)
				continue
			}
		}

		return session
	}
}

func triggerJob(jobName string) *gexec.Session {
	return start(exec.Command(
		flyBin,
		"-t",
		targetedConcourse,
		"trigger-job",
		"-j", pipelineName+"/"+jobName,
		"-w",
	))
}

func abortBuild(jobName string, build int) {
	sess := start(exec.Command(
		flyBin,
		"-t",
		targetedConcourse,
		"abort-build",
		"-j", pipelineName+"/"+jobName,
		"-b", strconv.Itoa(build),
	))
	<-sess.Exited
	Expect(sess).To(gexec.Exit(0))
}

func triggerPipelineJob(pipeline string, jobName string) *gexec.Session {
	return start(exec.Command(
		flyBin,
		"-t",
		targetedConcourse,
		"trigger-job",
		"-j", pipeline+"/"+jobName,
		"-w",
	))
}

func start(cmd *exec.Cmd) *gexec.Session {
	session, err := gexec.Start(
		cmd,
		gexec.NewPrefixedWriter(
			fmt.Sprintf("%s%s ", ansi.Color("[o]", "green"), ansi.Color("[fly]", "blue")),
			GinkgoWriter,
		),
		gexec.NewPrefixedWriter(
			fmt.Sprintf("%s%s ", ansi.Color("[e]", "red+bright"), ansi.Color("[fly]", "blue")),
			GinkgoWriter,
		),
	)
	Expect(err).NotTo(HaveOccurred())

	return session
}

func flyTable(argv ...string) []map[string]string {
	session := start(exec.Command(
		flyBin,
		append([]string{"-t", targetedConcourse, "--print-table-headers"}, argv...)...,
	))
	<-session.Exited
	Expect(session.ExitCode()).To(Equal(0))

	result := []map[string]string{}
	var headers []string

	rows := strings.Split(string(session.Out.Contents()), "\n")
	for i, row := range rows {
		if i == 0 {
			headers = splitFlyColumns(row)
			continue
		}
		if row == "" {
			continue
		}

		result = append(result, map[string]string{})
		columns := splitFlyColumns(row)

		Expect(columns).To(HaveLen(len(headers)))

		for j, header := range headers {
			if header == "" || columns[j] == "" {
				continue
			}

			result[i-1][header] = columns[j]
		}
	}

	return result
}

func splitFlyColumns(row string) []string {
	return regexp.MustCompile(`\s{2,}`).Split(strings.TrimSpace(row), -1)
}
