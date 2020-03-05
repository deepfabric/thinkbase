package value

import (
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/deepfabric/thinkbase/pkg/util/encoding"
)

func TestValue(t *testing.T) {
	{
		var v0, v1 []interface{}

		v0 = append(v0, *NewInt(123))
		v0 = append(v0, *NewFloat(124.545))
		v0 = append(v0, *NewBool(true))
		v0 = append(v0, *NewString("fabg"))
		v0 = append(v0, ConstNull)
		{
			var v3 []interface{}

			v3 = append(v3, *NewTime(time.Now()))
			v3 = append(v3, *NewTable("3434"))
			{
				var v4 []interface{}
				v4 = append(v4, ConstNull)
				v4 = append(v4, *NewString("xxxx"))
				v3 = append(v3, v4)
			}
			v0 = append(v0, v3)
		}

		data, err := encoding.Encode(v0)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v\n", v0)
		if err := encoding.Decode(data, &v1); err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%v\n", v1)
	}
}
