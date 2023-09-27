DROP PROCEDURE UPDATE_LEADERS_SCORE;
DELIMITER //

CREATE PROCEDURE UPDATE_LEADERS_SCORE(IN in_School_ID INT, IN in_Leader_Score INT)

LANGUAGE SQL
MODIFIES SQL DATA

BEGIN
    UPDATE chicago_public_schools
	SET Leaders_Score = in_Leader_Score
	WHERE School_ID = in_School_ID;
	
	IF in_Leader_Score > 0 AND in_Leader_Score < 20 THEN
		UPDATE chicago_public_schools
		SET Leaders_Icon = 'Very weak';
	
	ELSEIF in_Leader_Score < 40 THEN
		UPDATE chicago_public_schools
		SET Leaders_Icon = 'Weak';
	
	ELSEIF in_Leader_Score < 60 THEN
		UPDATE chicago_public_schools
		SET Leaders_Icon = 'Average';
	
	ELSEIF in_Leader_Score < 80 THEN
		UPDATE chicago_public_schools
		SET Leaders_Icon = 'Strong';
	
	ELSEIF in_Leader_Score < 100 THEN
		UPDATE chicago_public_schools
		SET Leaders_Icon = 'Very strong';

	ELSE
        ROLLBACK WORK;
    
END IF;

COMMIT WORK;

END //

DELIMITER ;

SET SQL_SAFE_UPDATES = 0;
CALL UPDATE_LEADERS_SCORE(610038,50);
SELECT School_ID,Leaders_Score FROM chicago_public_schools;

CALL UPDATE_LEADERS_SCORE(610038,38);
SELECT School_ID,Leaders_Score FROM chicago_public_schools;

CALL UPDATE_LEADERS_SCORE(61038,101);
SELECT School_ID,Leaders_Score FROM chicago_public_schools;