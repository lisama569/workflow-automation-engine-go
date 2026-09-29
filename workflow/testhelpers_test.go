package workflow

// serialThreeSteps builds a trivial 3-step serial definition sharing activity.
func serialThreeSteps(activity string) Definition {
	return Definition{
		Name: "serial-" + activity,
		Steps: []StepDef{
			{Name: "a", Activity: activity},
			{Name: "b", Activity: activity, DependsOn: []string{"a"}},
			{Name: "c", Activity: activity, DependsOn: []string{"b"}},
		},
	}
}
