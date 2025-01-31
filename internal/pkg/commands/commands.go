package commands

const (
	CmdIdTerminate  = 1
	CmdIdPauseIncr  = 2
	CmdIdResumeIncr = 3
	CmdIdSnapshot   = 4
)

type Command struct {
	Id        int
	Arguments []string
}

var (
	CmdTerminate         = Command{Id: CmdIdTerminate}
	CmdPauseIncremental  = Command{Id: CmdIdPauseIncr}
	CmdResumeIncremental = Command{Id: CmdIdResumeIncr}
)

func NewCmdSnapshot(database string, collection string) Command {
	args := []string{database, collection}
	return Command{
		Id:        CmdIdSnapshot,
		Arguments: args,
	}
}
