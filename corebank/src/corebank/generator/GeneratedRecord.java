package corebank.generator;
import java.util.*;

/** Transport-neutral field values; IDs and amounts remain exact longs. */
public record GeneratedRecord(String kind, String key, List<String> names, List<Object> values, List<Integer> widths) {
    public GeneratedRecord {
        names=List.copyOf(names);
        values=Collections.unmodifiableList(new ArrayList<>(values));
        widths=List.copyOf(widths);
        if (names.size()!=values.size() || names.size()!=widths.size()) throw new IllegalArgumentException("Field sizes differ");
    }
    public Map<String,Object> fields() {
        var result=new LinkedHashMap<String,Object>();
        for(int i=0;i<names.size();i++) result.put(names.get(i),values.get(i));
        return result;
    }
    public String line() {
        var result=new StringBuilder();
        for(int i=0;i<values.size();i++) {
            Object value=values.get(i); int width=widths.get(i);
            if(value instanceof Number) {
                String text=value.toString(); result.append(" ".repeat(width-text.length())).append(text);
            } else result.append(Generate.field(value==null?"":value.toString(),width));
        }
        return result.toString();
    }
}
