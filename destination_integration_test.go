// Copyright © 2022 Meroxa, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"context"
	"github.com/matryer/is"
	"testing"
)

func TestDestination_Integration_WriteTopic(t *testing.T) {
	cfg := DestinationConfigMap(*t)
	is := is.New(t)
	ctx := context.Background()

	wantRecords := GenerateSDKRecords(1, 6)

	underTest := NewDestination()
	defer func() {
		err := underTest.Teardown(ctx)
		is.NoErr(err)
	}()

	err := underTest.Configure(ctx, cfg)
	is.NoErr(err)

	err = underTest.Open(ctx)
	is.NoErr(err)
	t.Logf("Writing now")

	count, err := underTest.Write(ctx, wantRecords)
	is.NoErr(err)
	is.Equal(count, len(wantRecords))

	gotRecords := Consume(*t, ParseConfigMap[Config](*t, cfg), len(wantRecords))
	is.Equal(len(wantRecords), len(gotRecords))
	for i, got := range gotRecords {
		is.Equal(got.Payload(), wantRecords[i].Bytes())
	}
}
