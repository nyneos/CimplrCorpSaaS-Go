package additionalfiles

import "testing"

func TestPackageZipName(t *testing.T) {
	if got := packageZipName([]string{"BS/001"}, "Bank Statement"); got != "BS_001.zip" {
		t.Fatalf("single zip name = %q", got)
	}
	if got := packageZipName([]string{"BS001", "BS002"}, "Bank Statement"); got != "Bank Statement.zip" {
		t.Fatalf("bulk zip name = %q", got)
	}
}

func TestSafeZipNames(t *testing.T) {
	if got := safeZipSegment(`bad/name\id`); got != "bad_name_id" {
		t.Fatalf("safe segment = %q", got)
	}
	if got := safeZipFileName(`..\bad:name?.csv`); got != "bad_name_.csv" {
		t.Fatalf("safe file = %q", got)
	}
}

func TestUniqueZipPath(t *testing.T) {
	used := map[string]int{}
	first := uniqueZipPath(used, "ROW/file.csv")
	second := uniqueZipPath(used, "ROW/file.csv")
	third := uniqueZipPath(used, "ROW/file.csv")
	if first != "ROW/file.csv" || second != "ROW/file (2).csv" || third != "ROW/file (3).csv" {
		t.Fatalf("unexpected paths: %q %q %q", first, second, third)
	}
}
