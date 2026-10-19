package compile

// runtimeBranchNodeName names an attached node under its branch.
func runtimeBranchNodeName(branchName string, localName string) string {
	return branchName + "/" + localName
}
