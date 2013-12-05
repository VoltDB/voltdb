package javax.annotation_voltpatches;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

import javax.annotation_voltpatches.meta.TypeQualifierNickname;
import javax.annotation_voltpatches.meta.When;

/** Used to annotate a value of unknown sign */

@Documented
@TypeQualifierNickname
@Nonnegative(when = When.UNKNOWN)
@Retention(RetentionPolicy.RUNTIME)
public @interface Signed {

}
