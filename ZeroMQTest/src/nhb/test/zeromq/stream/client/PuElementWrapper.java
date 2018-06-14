package nhb.test.zeromq.stream.client;

import com.nhb.common.data.PuElement;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class PuElementWrapper {

	private PuElement data;
}
