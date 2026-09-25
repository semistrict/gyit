package control

func logParentLengths(parents []string) []byte {
	if len(parents) == 0 {
		return nil
	}
	lengths := make([]byte, len(parents))
	for i, parent := range parents {
		lengths[i] = byte(len(parent))
	}
	return lengths
}
